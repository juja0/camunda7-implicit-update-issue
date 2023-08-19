package org.camunda.bpm.unittest;

import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbEntityOperation;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbOperationManager;

import java.util.*;
import java.util.function.Function;

import static org.camunda.bpm.engine.impl.db.entitymanager.operation.DbOperationType.INSERT;

public class FastDbOperationManager extends DbOperationManager
{
	private static final int FAST_DB_OPERATIONS_THRESHOLD = 64;
	private static final boolean USE_FAST_DB_OPERATIONS_SORT = true;
	private static final boolean FALLBACK_TO_DEFAULT_DB_OPERATIONS_SORT = false;

	private static final Function<String, Set<String>> SET_FACTORY = k -> new HashSet<>();

	/**
	 * Sort the incoming set of DbEntityOperations using topological sort.
	 * <p>
	 * The idea is to first scan all the operations and prepare a map of
	 * dependents and also find the number of pre-requisites for each operation.
	 * <p>
	 * dependents map will hold information about the entity ids that are dependent on each entity
	 * i.e the keys are entityIds and the values are the set of entityIds to be executed after the key entityId.
	 * <p>
	 * prerequisitesCount map will hold information about the number of operations that need to be executed before
	 * the current one can be executed. An entity-operation with a prerequisites count of zero means it does not have
	 * any dependencies and can be executed immediately.
	 * <p>
	 * Once the dependents and prerequisitesCount have been computed, in the next step, we start with
	 * operations with no prerequisites (i.e, prerequisitesCount = 0) and execute all of them. After
	 * executing each, we find all the operations that depend on the current one and reduce their
	 * prerequisitesCount by one. By doing this, we whittle away at the pre-requisites of future
	 * operations till they themselves become eligible for execution when the count reaches 0.
	 * <p>
	 * When this happens for an entity-operation (i.e, prerequisitesCount reaches 0), it's queued up
	 * for the next round of processing. This is done till the processing queue (the variable 'toVisit') is empty.
	 * When the processing queue is empty, the resultant list of operations is properly sorted with
	 * dependencies being respected.
	 */
	@Override
	protected List<DbEntityOperation> sortByReferences(SortedSet<DbEntityOperation> preSorted)
	{
		int size = preSorted.size();

		if (!USE_FAST_DB_OPERATIONS_SORT || size < FAST_DB_OPERATIONS_THRESHOLD)
		{
			return super.sortByReferences(preSorted);
		}

		List<DbEntityOperation> opList = new ArrayList<>(size);

		Map<String, DbEntityOperation> all = new HashMap<>();
		Map<String, Set<String>> dependents = new HashMap<>();
		Map<String, Integer> prerequisitesCount = new HashMap<>();

		// scan all operations and prepare dependents and prerequisite counts
		for (DbEntityOperation operation : preSorted)
		{
			String current = operation.getEntity().getId();
			Set<String> refs = operation.getFlushRelevantEntityReferences();

			if (all.put(current, operation) != null)
			{
				// is this valid ?
				return this.fallBackOrThrow(preSorted, "Error calculating operations to persist. Multiple operations found for entity.");
			}

			if (!prerequisitesCount.containsKey(current))
			{
				prerequisitesCount.put(current, 0);
			}

			if (refs != null)
			{
				for (String ref : refs)
				{
					if (!Objects.equals(current, ref))
					{
						if (!prerequisitesCount.containsKey(ref))
						{
							prerequisitesCount.put(ref, 0);
						}

						if (operation.getOperationType() == INSERT)
						{
							// current is dependent on ref
							// i.e. current should be inserted after ref
							if (dependents.computeIfAbsent(ref, SET_FACTORY).add(current))
							{
								prerequisitesCount.put(current, prerequisitesCount.getOrDefault(current, 0) + 1);
							}
						}
						else
						{
							// ref is dependent on current
							// i.e. ref should be updated/deleted after current
							if (dependents.computeIfAbsent(current, SET_FACTORY).add(ref))
							{
								prerequisitesCount.put(ref, prerequisitesCount.getOrDefault(ref, 0) + 1);
							}
						}
					}
				}
			}
		}

		Queue<String> toVisit = new LinkedList<>();

		// populate queue with operations eligible for first round of processing
		// i.e, operations with no pre-requisites
		for (Map.Entry<String, Integer> entry : prerequisitesCount.entrySet())
		{
			if (entry.getValue() == 0)
			{
				toVisit.offer(entry.getKey());
			}
		}

		// process queue till it's empty
		while (!toVisit.isEmpty())
		{
			Queue<String> curLevel = new LinkedList<>(toVisit);
			toVisit.clear();
			while (!curLevel.isEmpty())
			{
				String cur = curLevel.poll();
				if (all.containsKey(cur))
				{
					opList.add(all.remove(cur)); // add the current operation to final list
				}
				// find dependents of current operation and reduce their pre-requisite count by one.
				// if any dependent ends up with pre-requisite count = 0, queue it up for processing in next round.
				for (String dependent : dependents.getOrDefault(cur, new HashSet<>()))
				{
					if (prerequisitesCount.containsKey(dependent))
					{
						prerequisitesCount.put(dependent, prerequisitesCount.get(dependent) - 1);
						if (prerequisitesCount.get(dependent) == 0)
						{
							prerequisitesCount.remove(dependent);
							toVisit.offer(dependent);
						}
					}
				}
			}
		}

		if (opList.size() != size)
		{
			// this is definitely not valid. indicates something wrong with the logic above or there may be a loop. investigate !!!
			return this.fallBackOrThrow(preSorted, "Error calculating operations to persist. Some operations are missing.");
		}

		return opList;
	}

	private List<DbEntityOperation> fallBackOrThrow(SortedSet<DbEntityOperation> preSorted, String message)
	{
		if (FALLBACK_TO_DEFAULT_DB_OPERATIONS_SORT)
		{
			return super.sortByReferences(preSorted);
		}
		throw new IllegalStateException(message);
	}
}

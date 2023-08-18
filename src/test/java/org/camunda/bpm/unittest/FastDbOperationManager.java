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

		for (Map.Entry<String, Integer> entry : prerequisitesCount.entrySet())
		{
			if (entry.getValue() == 0)
			{
				toVisit.offer(entry.getKey());
			}
		}

		while (!toVisit.isEmpty())
		{
			Queue<String> curLevel = new LinkedList<>(toVisit);
			toVisit.clear();
			while (!curLevel.isEmpty())
			{
				String cur = curLevel.poll();
				if (all.containsKey(cur))
				{
					opList.add(all.remove(cur));
				}
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

package org.camunda.bpm.unittest;

import org.camunda.bpm.engine.impl.cfg.IdGenerator;
import org.camunda.bpm.engine.impl.db.PersistenceSession;
import org.camunda.bpm.engine.impl.db.entitymanager.DbEntityManager;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbOperationManager;

public class ExtendedDbEntityManager extends DbEntityManager
{
	public ExtendedDbEntityManager(IdGenerator idGenerator, PersistenceSession ps)
	{
		super(idGenerator, ps);
	}

	@Override
	protected void initializeOperationManager()
	{
		this.dbOperationManager = new FastDbOperationManager();
	}

	@Override
	public void setDbOperationManager(DbOperationManager operationManager)
	{
		throw new UnsupportedOperationException();
	}
}

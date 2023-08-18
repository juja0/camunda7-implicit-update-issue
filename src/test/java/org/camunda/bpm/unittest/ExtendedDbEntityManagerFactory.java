package org.camunda.bpm.unittest;

import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.db.PersistenceSession;
import org.camunda.bpm.engine.impl.db.entitymanager.DbEntityManager;
import org.camunda.bpm.engine.impl.db.entitymanager.DbEntityManagerFactory;

class ExtendedDbEntityManagerFactory extends DbEntityManagerFactory
{
	public ExtendedDbEntityManagerFactory(ProcessEngineConfigurationImpl config)
	{
		super(config.getIdGenerator());
	}

	@Override
	public DbEntityManager openSession()
	{
		return new ExtendedDbEntityManager(this.idGenerator, getPersistenceSession());
	}

	private static PersistenceSession getPersistenceSession()
	{
		return Context.getCommandContext().getSession(PersistenceSession.class);
	}
}

package com.aws.dcortesi.hive;

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * QueryValidatorHook can intercept an incoming query to validate certain aspects of it
 * before getting compiled by the Hive Driver.
 */
public class QueryValidatorHook implements ExecuteWithHookContext {
    private static final Logger log = LoggerFactory.getLogger(Sweeper.class);

    public void run(HookContext hookContext) throws Exception {
        QueryPlan plan = hookContext.getQueryPlan();
        String query = plan.getQueryStr();

        // Grab the console to send messages to the user
        SessionState.LogHelper console = SessionState.getConsole();
        if (console != null) {
            console.printInfo("PREHOOK: query: " + query);
        }

        // NOTE: This is _not_ a secure means of validating the query ... just an example. :)
        if (query.matches("(?i).*insert.*")) {
            throw new RuntimeException("Cannot execute insert statements");
        }

        log.info("QueryValidator received query string: " + query);
    }
}

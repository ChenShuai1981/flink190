//package com.caselchen.flink;
//
//import org.kie.api.runtime.KieSession;
//
///**
// * Responsible for getting a Drools session and applying the Drools rules.
// */
//public class DroolsRulesApplier {
//    private static KieSession KIE_SESSION;
//
//    public DroolsRulesApplier(String sessionName) {
//        KIE_SESSION = DroolsSessionFactory.createDroolsSession(sessionName);
//    }
//
//    /**
//     * Applies the loaded Drools rules to a given String.
//     *
//     * @param person the data to which the rules should be applied
//     * @return the String after the rule has been applied
//     */
//    public String applyRule(Person person) {
//        KIE_SESSION.insert(person);
//        KIE_SESSION.fireAllRules();
//        return person.getName();
//    }
//}
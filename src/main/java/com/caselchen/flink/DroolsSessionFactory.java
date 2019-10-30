package com.caselchen.flink;

import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.Message;
import org.kie.api.io.Resource;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;

import java.io.ByteArrayInputStream;
import java.io.StringReader;

/**
 * Instantiates the Drools KieSession, responsible for applying rules.
 */
public class DroolsSessionFactory {

//    /**
//     * Creates a new KieSession instance, configured with the rules from the given session.
//     *
//     * @param sessionName the session name from which rules should be retrieved
//     * @return the instantiated KieSession
//     */
//    public static KieSession createDroolsSession(String sessionName) {
//        KieServices kieServices = KieServices.Factory.get();
//        KieContainer kieContainer = kieServices.getKieClasspathContainer();
//        return kieContainer.newKieSession(sessionName);
//    }

    public static KieSession createDroolsSessionFromDrl(String drl) {
        KieServices ks = KieServices.Factory.get();
        KieRepository kr = ks.getRepository();
        KieFileSystem kfs = ks.newKieFileSystem();
        kfs.write( "src/main/resources/simple.drl",
                ks.getResources().newReaderResource( new StringReader(drl) ) );
        KieBuilder kb = ks.newKieBuilder( kfs ).buildAll();
        if (kb.getResults().hasMessages(Message.Level.ERROR)) {
            throw new RuntimeException("Build Errors:\n" + kb.getResults().toString());
        }

        KieContainer kContainer = ks.newKieContainer(kr.getDefaultReleaseId());

        KieSession kSession = kContainer.newKieSession();
//        kSession.setGlobal("out", out);
        return kSession;
    }
}
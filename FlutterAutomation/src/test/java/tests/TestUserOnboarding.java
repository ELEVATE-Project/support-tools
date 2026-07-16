package tests;

import base.BaseTest;

import org.testng.annotations.Test;
import data.TestData;
// import flows.ProfileFlow;
import flows.RegistrationFlow;

public class TestUserOnboarding extends BaseTest {

    @Test
    public void launchApp() throws Exception {

        System.out.println("===== APP LAUNCHED =====");

        Thread.sleep(10000);

        RegistrationFlow registrationFlow =
                new RegistrationFlow(driver);

        registrationFlow.register(
                TestData.Register_EMAIL,
                TestData.FIRST_NAME,
                TestData.MIDDLE_NAME,
                TestData.LAST_NAME,
                TestData.EMAIL,
                TestData.PASSWORD
        );

        System.out.println("===== REGISTRATION COMPLETED & LOGIN SUCCESSFUL =====");

        Thread.sleep(15000);


        // Login Flow


        // LoginFlow loginFlow = new LoginFlow(driver);

        
        // loginFlow.login(
        //         TestData.EMAIL,
        //         TestData.PASSWORD
        // );

        // System.out.println("===== LOGIN SUCCESSFUL =====");

        // Thread.sleep(10000);

        // // Profile Update Flow
        // ProfileFlow profileFlow = new ProfileFlow(driver);

        // profileFlow.updateAcademicInformation();

        // System.out.println("===== PROFILE UPDATED SUCCESSFULLY =====");

        // Thread.sleep(5000);

    }
}
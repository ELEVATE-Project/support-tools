package tests;

import base.BaseTest;

import org.testng.annotations.Test;

import data.TestData;
import flows.LoginFlow;
import flows.ProfileFlow;

public class TestUserProfile extends BaseTest {

    @Test
    public void updateProfile() throws Exception {

        System.out.println("===== APP LAUNCHED =====");

        Thread.sleep(10000);

        // Login Flow
        LoginFlow loginFlow = new LoginFlow(driver);

        loginFlow.login(
                TestData.EMAIL,
                TestData.PASSWORD
        );

        System.out.println("===== LOGIN SUCCESSFUL =====");

        Thread.sleep(10000);

        // Profile Update Flow
        ProfileFlow profileFlow = new ProfileFlow(driver);

        profileFlow.updateAcademicInformation();

        System.out.println("===== PROFILE UPDATED SUCCESSFULLY =====");

        Thread.sleep(10000);
    }
}
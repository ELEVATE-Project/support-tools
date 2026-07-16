package flows;

import base.BaseFlow;
import io.appium.java_client.android.AndroidDriver;
import pages.LanguagePage;
import pages.LoginPage;
import pages.RegistrationPage;
import pages.RoleSelectionPage;
import pages.StudentProfilePage;
import pages.UserTypePage;

public class RegistrationFlow extends BaseFlow {

    private LanguagePage languagePage;
    private UserTypePage userTypePage;
    private RoleSelectionPage rolePage;
    private StudentProfilePage profilePage;
    private RegistrationPage registrationPage;
    private LoginPage loginPage;

    public RegistrationFlow(AndroidDriver driver) {

        super(driver);

        languagePage = new LanguagePage(driver);
        userTypePage = new UserTypePage(driver);
        rolePage = new RoleSelectionPage(driver);
        profilePage = new StudentProfilePage(driver);
        registrationPage = new RegistrationPage(driver);
        loginPage = new LoginPage(driver);
    }

    public void register(
            String registerEmail,
            String firstName,
            String middleName,
            String lastName,
            String email,
            String password
    ) throws Exception {

        /* ---------------- LANGUAGE ---------------- */

        log("SELECTING LANGUAGE");

        languagePage.selectEnglish();
        languagePage.clickContinue();

        /* ---------------- USER TYPE ---------------- */

        log("SELECTING NEW USER");

        userTypePage.clickNewUser();

        /* ---------------- ROLE ---------------- */

        log("SELECTING ROLE");

        rolePage.selectOfficials();

        /* ---------------- STUDENT PROFILE ---------------- */

        log("SELECTING BOARD");
        printPageSource();
        Thread.sleep(2000);
        profilePage.clickBoard();
        Thread.sleep(2000);
        profilePage.selectCBSE();
        Thread.sleep(2000);
        profilePage.clickSubmit();

        log("SELECTING MEDIUM");

        profilePage.clickMedium();
        profilePage.selectEnglish();
        profilePage.clickSubmit();

        log("SELECTING CLASS");

        profilePage.clickClass();
        profilePage.selectClass8();
        profilePage.clickSubmit();

        profilePage.clickContinue();

        log("PROFILE DETAILS COMPLETED");

        printPageSource();

        /* ---------------- REGISTER ---------------- */

        Thread.sleep(10000);
        loginPage.ClickRegister();

        log("REGISTER PAGE OPENED");

        registrationPage.enterFirstName(firstName);

        registrationPage.enterMiddleName(middleName);

        registrationPage.enterLastName(lastName);

        registrationPage.selectDOB2000();

        registrationPage.selectRole();

        registrationPage.selectGender();

        registrationPage.selectEmail();

        registrationPage.enterEmail(registerEmail);

        registrationPage.clickGenOTP();

        log("ENTER OTP MANUALLY");

        Thread.sleep(40000);

        registrationPage.enterPassword(password);

        registrationPage.enterConfirmPassword(password);

        registrationPage.clickAgree();

        registrationPage.clickRegister();

        log("REGISTRATION COMPLETED");

        /* ---------------- LOGIN ---------------- */

        log("LOGGING IN");

        loginPage.EnterEmail(email);

        loginPage.EnterPassword(password);

        Thread.sleep(2000);
        loginPage.clickLogin();

        log("LOGIN SUCCESSFUL");

        Thread.sleep(10000);
    }
}
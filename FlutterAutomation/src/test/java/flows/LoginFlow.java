package flows;

import base.BaseFlow;
import io.appium.java_client.android.AndroidDriver;
import pages.LanguagePage;
import pages.LoginPage;
import pages.UserTypePage;

public class LoginFlow extends BaseFlow {

    private LanguagePage languagePage;
    private UserTypePage userTypePage;
    private LoginPage loginPage;

    public LoginFlow(AndroidDriver driver) {

        super(driver);

        languagePage = new LanguagePage(driver);
        userTypePage = new UserTypePage(driver);
        loginPage = new LoginPage(driver);
    }

    public void login(String email, String password) throws Exception {

        log("SELECTING LANGUAGE");

        languagePage.selectEnglish();
        languagePage.clickContinue();

        log("OPENING LOGIN PAGE");

        userTypePage.clickExistingUser();

        log("LOGIN PAGE OPENED");

        printPageSource();

        log("ENTERING EMAIL");

        loginPage.EnterEmail(email);

        log("ENTERING PASSWORD");

        loginPage.EnterPassword(password);

        log("CLICKING LOGIN");

        Thread.sleep(5000);
        loginPage.clickLogin();

        log("LOGIN COMPLETED");
    }
}
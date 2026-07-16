package pages;

import base.BasePage;
import io.appium.java_client.AppiumBy;
import io.appium.java_client.android.AndroidDriver;

public class LoginPage extends BasePage{

        public LoginPage(AndroidDriver driver) {
                super(driver);
        }

        public void ClickRegister(){
                click(
                        AppiumBy.xpath("//android.view.View[@content-desc=\"Register here\"]")
                );
        }

        public void EnterEmail(String email) {
                type(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"username\")"
                        ), email
                );
        }

        public void EnterPassword(String password) {
                type(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"password\")"
                        ), password
                );
        }

        public void clickLogin() {
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"login\")"
                        )
                );
        }

}
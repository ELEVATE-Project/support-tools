package pages;

import io.appium.java_client.android.AndroidDriver;
import org.openqa.selenium.By;

import base.BasePage;

public class UserTypePage extends BasePage{

    public UserTypePage(AndroidDriver driver) {
        super(driver);
    }

    public void clickNewUser() {

        click(
            By.xpath("//*[@content-desc='New User']")
        );
    }

    public void clickExistingUser() {

        click(
            By.xpath("//*[@content-desc='Existing User']")
        );
    }
}
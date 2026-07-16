package pages;


import base.BasePage;
import io.appium.java_client.AppiumBy;
import io.appium.java_client.android.AndroidDriver;

public class HomePage extends BasePage{
    public HomePage(AndroidDriver driver) {
        super(driver);
    }


    public void openProfile() {
        click(
            AppiumBy.androidUIAutomator("new UiSelector().description(\"T\")")
        );
    }

    

}

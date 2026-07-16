package pages;

import org.openqa.selenium.By;

import base.BasePage;
import io.appium.java_client.android.AndroidDriver;

public class RoleSelectionPage extends BasePage{

    public RoleSelectionPage(AndroidDriver driver) {
        super(driver);
    }

    public void selectStudent() {
        click(
            By.xpath("//*[@content-desc='Student']")
        );
    }

    public void selectTeacher() {
        click(
            By.xpath("//*[@content-desc='Teacher']")
        );
    }

    public void selectParent() {
        click(
            By.xpath("//*[@content-desc='Parent']")
        );
    }

    public void selectOfficials() {
        click(
            By.xpath("//*[@content-desc='HT & Officials']")
        );
    }
}
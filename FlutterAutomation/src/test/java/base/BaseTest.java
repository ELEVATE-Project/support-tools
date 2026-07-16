package base;

import config.Config;

import io.appium.java_client.android.AndroidDriver;
import io.appium.java_client.android.options.UiAutomator2Options;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.net.URL;

public class BaseTest {

    protected AndroidDriver driver;

    @BeforeMethod
    public void setUp() throws Exception {

        UiAutomator2Options options =
                new UiAutomator2Options();

        options.setPlatformName(
                Config.PLATFORM_NAME
        );

        options.setDeviceName(
                Config.DEVICE_NAME
        );

        options.setUdid(
                Config.UDID
        );

        options.setAppPackage(
                Config.APP_PACKAGE
        );

        options.setAppActivity(
                Config.APP_ACTIVITY
        );

        driver =
                new AndroidDriver(
                        new URL(Config.APPIUM_SERVER),
                        options
                );
    }

    @AfterMethod
    public void tearDown() {

        if (driver != null) {
            driver.quit();
        }
    }
}
package pages;
import base.BasePage;

import io.appium.java_client.AppiumBy;
import io.appium.java_client.android.AndroidDriver;

public class ObservationWRPage extends BasePage{
        public ObservationWRPage(AndroidDriver driver) {
            super(driver);
        }
        
        public void clickObs() {
            click(
                AppiumBy.xpath(
                    "//android.webkit.WebView[@text=\"Diksha\"]/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View/android.widget.ListView[2]/android.view.View[2]/android.view.View"
                )
            );
        }

        public void clickObsWOR(String ObservationWORName) {
            click(
                AppiumBy.xpath(
                        "//android.widget.TextView[@text=\"" + ObservationWORName + "\"]"
                )
            );
        }

        public void clickAddSchool() {
            click(
                AppiumBy.xpath(
                    "//android.widget.Button[@text=\"Add school\"]"
                )
            );
        }

        public void selectSchool(){
            click(
                AppiumBy.xpath("//android.view.View[@text=\"TRIPURA LOKASHIKSHALAYA HIGH SCHOOL, 16011003504\"]"                  
                )
            );

            click(
                AppiumBy.xpath(
                    "//android.widget.Button[@text=\"Add\"]"
                )
            );
        }
        public void clickStartObservation() {
            click(
                AppiumBy.xpath(
                    "//android.widget.TextView[@text=\"TRIPURA LOKASHIKSHALAYA HIGH SCHOOL\"]"
                )
            );
        }

        public void clickObservation1(){
            click(
                AppiumBy.xpath("//android.widget.TextView[@text=\"Observation 1\"]"
                )
            );
        }

        public void select1stDomain(){
            click(
                AppiumBy.xpath("//android.widget.TextView[@text=\"Domain 1\"]"
                )
            );

            click(
                AppiumBy.xpath("//android.widget.TextView[@text=\"Planning & Execution\"]"
                )
            );
        }

        public void consume1stDomain1Question(){

            type(
                AppiumBy.xpath("//android.widget.EditText[@resource-id=\"mat-input-2\"]"
                ),
                "Test Observation WR Flow"
            );
        }

        public void consume1stDomain2Question(){
            scrollDown();
            
            click(
                AppiumBy.xpath("//android.view.View[@resource-id=\"6a4233e27c50c80514715566R1\"]/android.view.View"
                )
            );

            scrollDown();
        }
        
        public void consume1stDomain3Question() {

            type(
                AppiumBy.id( "mat-input-3"),
                "4"
            );
        }

        public void consume1stDomain4Question(){
            scrollDown();
            click(
                AppiumBy.xpath("//android.view.View[@resource-id=\"6a4233e27c50c81650715558R5\"]"
                )
            );
        }

        public void submit1stDomainResponse(){
            scrollDown();
            click(
                AppiumBy.xpath("//android.widget.Button[@text=\"Submit\"]"
                )
            );
        }

        public void confirmSubmit1stDomainResponse(){
            click(
                AppiumBy.xpath("//android.widget.Button[@text=\"Confirm\"]"
                )
            );
        }

        public void click2ndDomain(){
            click(
                AppiumBy.xpath("//android.widget.TextView[@text=\"Domain 2\"]"
                )
            );

            click(
                AppiumBy.xpath("//android.widget.TextView[@text=\"Data based Governance\"]"
                )
            );
        }

        public void consume2ndDomain1Question(){
            click(
                AppiumBy.xpath("//android.view.View[@resource-id=\"6a4233e27c50c8ac0971555eR2\"]"
                )
            );
        }

        public void consume2ndDomain2Question(){
            scrollDown();

            click(
                AppiumBy.xpath("//android.view.View[@resource-id=\"6a4233e27c50c8385e715562R2\"]/android.view.View"
                )
            );
        }

        public void consume2ndDomain3Question(){
            scrollDown();
            click(
                AppiumBy.xpath("//android.view.View[@resource-id=\"6a4233e27c50c80b9671555aR2\"]"
                )
            );
        }

        public void submit2ndDomain4Response() throws Exception {
            scrollDown();
            click(
                AppiumBy.xpath("//android.widget.Button[@content-desc=\"Open calendar\"]"
                )
            );
            click(
                AppiumBy.xpath("//android.widget.ToggleButton[@text='3 July 2026']")
            );
        }

        public void selectnextpage(){
            click(
                AppiumBy.xpath("//android.widget.Button[@text=\"2\"]"
                )
            );
        }

        public void submit2ndDomain5Response(){
            click(
                AppiumBy.xpath("//android.view.View[@resource-id=\"mat-mdc-checkbox-6\"]/android.view.View"
                )
            );
        }

        public void submit2ndDomainResponse(){
            scrollDown();
            click(
                AppiumBy.xpath("//android.widget.Button[@text=\"Submit\"]"
                )
            );
        }

        public void confirmSubmit2ndDomainResponse(){
            click(
                AppiumBy.xpath("//android.widget.Button[@text=\"Confirm\"]"
                )
            );
        }

        public void click3rdDomain(){
            click(
                AppiumBy.xpath("//android.widget.TextView[@text=\"Domain 3\"]"
                )
            );

            click(
                AppiumBy.xpath("//android.widget.TextView[@text=\"Communication\"]"
                )
            );
        }

        public void consume3rdDomain1Question(){
            click(
                AppiumBy.xpath("//android.view.View[@resource-id=\"6a4233e27c50c85ccb715563R2\"]"
                )
            );
        }

        public void submit3rdDomainResponse(){
            scrollDown();
            click(
                AppiumBy.xpath("//android.widget.Button[@text=\"Submit\"]"
                )
            );
        }

        public void confirmSubmit3rdDomainResponse(){
            click(
                AppiumBy.xpath("//android.widget.Button[@text=\"Confirm\"]"
                )
            );
        }

        public void click4thDomain(){
            click(
                AppiumBy.xpath("//android.widget.TextView[@text=\"Domain 4\"]"
                )
            );

            click(
                AppiumBy.xpath("//android.widget.TextView[@text=\"Influence\"]"
                )
            );
        }

        public void consume4thDomain1Question(){
            click(
                AppiumBy.xpath("//android.view.View[@resource-id=\"6a4233e27c50c8487771555fR2\"]/android.view.View"
                )
            );
        }

        public void consume4thDomain2Question(){
            scrollDown();
            click(
                AppiumBy.xpath("//android.view.View[@resource-id=\"6a4233e27c50c8d786715565R2\"]/android.view.View"
                )
            );
        }

        public void submit4thDomainResponse(){
            scrollDown();
            click(
                AppiumBy.xpath("//android.widget.Button[@text=\"Submit\"]"
                )
            );
        }

        public void confirmSubmit4thDomainResponse(){
            click(
                AppiumBy.xpath("//android.widget.Button[@text=\"Confirm\"]"
                )
            );
        }

        // public void selectcompletedObservationWOR() {
        //     click(
        //         AppiumBy.xpath(
        //             "//android.widget.Button[@text=\"Completed\"]"
        //         )
        //     );
        // }

        // public void confirmsubmit() {
        //     click(
        //         AppiumBy.xpath(
        //             "//android.widget.Button[@text=\"Confirm\"]"
        //         )
        //     );
        // }

        public void seecompletedObservationWR() {
            click(
                AppiumBy.xpath(
                    "//android.view.View[@text='Completed' and @clickable='true']"
                )
            );
        }

        public void clickReports() {
            click(
                AppiumBy.xpath(
                    "//android.widget.Button[@text=\"Reports\"]"
                )
            );
        }

}
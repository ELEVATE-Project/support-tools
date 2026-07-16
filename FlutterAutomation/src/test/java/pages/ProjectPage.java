package pages;

import io.appium.java_client.AppiumBy;
import io.appium.java_client.android.AndroidDriver;

import base.BasePage;

public class ProjectPage extends BasePage {

        public ProjectPage(AndroidDriver driver) {
                super(driver);
        }

        public void clickProgramCard() {

                click(
                        AppiumBy.xpath("//android.widget.TextView[@text='Programs']")
                );
        }

        public void searchProgram(String programName) {

                type(
                        AppiumBy.xpath("//android.widget.EditText"),
                        programName
                );
        }

        public void clickProgram(String programName) {

                click(
                        AppiumBy.xpath("//android.widget.TextView[@text='" + programName + "']")
                );
        }

        public void clickProgramCardByName() {

                click(
                        AppiumBy.xpath("//android.widget.TextView[@text='Projects']")
                );
        }

        public void clickProject(String projectName) {

                click(
                        AppiumBy.xpath("//android.widget.TextView[@text='" + projectName + "']")
                );
        }

        public void startProject() {

                click(
                        AppiumBy.xpath("//android.widget.Button[@text='Start Improvement']")
                );
        }

        public void clickTaskDetails() {

                click(
                        AppiumBy.xpath(
                        "//android.view.View[@text='Task details' and @clickable='true']"
                        )
                );
        }

        public void clickTask1() {

                click(
                        AppiumBy.xpath(
                                "//android.widget.TextView[@text=\"1. Collect and analyze enrolment data from last 5 years.\"]"
                        )
                );
        }

        public void clickUploadEvidence(){

                click(
                        AppiumBy.xpath(
                                "//android.widget.Button[@text='Add files']"
                                )
                );

        }

        public void checkoboxuploadEvidence(){
                System.out.println(driver.getPageSource());
                click(
                AppiumBy.xpath(
                        "//android.app.AlertDialog//android.widget.CheckBox"
                )
                );

                click(
                        AppiumBy.xpath(
                                "//android.widget.Button[@text=\"Upload\"]"
                        )
                );

        }

        public void selectFileToUpload() {

                click(
                        AppiumBy.xpath(
                                "//android.webkit.WebView[@text=\"Diksha\"]/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View[3]"
                        )
                );
        }

        public void clickImageToUpload() {

                click(
                        AppiumBy.xpath(
                                "(//android.widget.ImageView[@resource-id=\"com.google.android.providers.media.module:id/icon_thumbnail\"])[1]"
                        )
                );
        }

        public void clickToAttachFile() {

                click(
                        AppiumBy.xpath(
                                "//android.widget.Button[@text=\"Attach files\"]"
                        )
                );
        }

        public void consumeTask1() {

                click(
                        AppiumBy.xpath("//android.widget.Spinner[@text='Not Started']")
                );

                click(
                        AppiumBy.xpath("//android.view.View[@text='Completed']")
                );
        }

        public void clickTask2() {

                click(
                        AppiumBy.xpath(
                                "//android.widget.TextView[@text=\"2. Conduct meeting with teachers and SMC members to discuss and set enrolment target for this year.\"]"
                        )
                );
        }

        public void consumeTask2() {

                click(
                        AppiumBy.xpath("//android.widget.Spinner[@text='Not Started']")
                );

                click(
                        AppiumBy.xpath("//android.view.View[@text='Completed']")
                );
                }

        public void clickTask3() {
                scrollDown();

                click(
                        AppiumBy.xpath("//android.widget.TextView[@text=\"3. Form a working committee and plan activities and timeline of enrolment program.\"]"
                        )
                );

        }

        public void consumeTask3() {

                click(
                        AppiumBy.xpath("//android.widget.Spinner[@text='Not Started']")
                );

                click(
                        AppiumBy.xpath("//android.view.View[@text='Completed']")
                );
        }

        public void clickTask4() {
                scrollDown();
                click(
                        AppiumBy.xpath(
                                "//android.widget.TextView[@text=\"4. Conduct different enrolment activities along with working committee.\"]"
                        )
                );
        }

        public void consumeTask4() {

                click(
                        AppiumBy.xpath("//android.widget.Spinner[@text='Not Started']")
                );

                click(
                        AppiumBy.xpath("//android.view.View[@text='Completed']")
                );
        }

        public void loadMoreButton() {
                scrollDown();
                scrollDown();
                click(
                        AppiumBy.xpath(
                                "//android.widget.Button[@text=\"Load more\"]"
                        )
                );
        }

        public void clickTask5() {
                scrollDown();
                click(
                        AppiumBy.xpath(
                                "//android.widget.TextView[@text=\"5. Calculate percentage increase in enrolment from last year and create an enrolment report.\"]"
                        )
                );
        }


        public void consumeTask5() {

                click(
                        AppiumBy.xpath("//android.widget.Spinner[@text='Not Started']")
                );

                click(
                        AppiumBy.xpath("//android.view.View[@text='Completed']")
                );
        }

        public void clickSubmitImprovement(){
                
                click(
                        AppiumBy.xpath(
                                "//android.widget.Button[@text=\"Submit Improvement\"]"
                        )
                );
        }

        public void projectCheckBoxUploadEvidence(){

                click(
                        AppiumBy.xpath(
                                "//android.app.AlertDialog//android.widget.CheckBox"
                        )
                );

                click(
                        AppiumBy.xpath(
                                "//android.widget.Button[@text=\"Upload\"]"
                        )
                );

        }

        public void confirmSubmit() {

                click(
                        AppiumBy.xpath(
                                "//android.widget.Button[@text=\"Submit\"]"
                        )
                );
        }

        public void clickCertificateCard() {

                click(
                        AppiumBy.xpath(
                                "//android.webkit.WebView[@text=\"Diksha\"]/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View[1]/android.view.View[3]/android.widget.TextView"
                        )
                );
        }

        public boolean isCertificateDisplayed() {

                return isDisplayed(
                        AppiumBy.xpath(
                        "//android.widget.TextView[@text='Certificate of Completion']"
                        )
                );
        }

}
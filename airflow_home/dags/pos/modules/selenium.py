from selenium import webdriver
from airflow import AirflowException
from selenium.webdriver import ActionChains
import time


def set_web_driver_profile_and_preferences(download_folder_path, driver_path, browser='chrome'):
    """
    Set the profile and preferences to force the download of
    certain type of files without popups into a folder.

    :param download_folder_path: A string indicating the folder to download files.

    :return: A selenium driver class.
    """
    if browser == 'firefox':
        profile = webdriver.FirefoxProfile()

        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.download.manager.showWhenStarting", False)
        profile.set_preference("browser.download.dir", download_folder_path)
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk",
                               "application/xls, application/vnd.ms-excel, text/plain, application/octet-stream, application/binary, text/csv, application/csv, application/excel, text/comma-separated-values, text/xml, application/xml")
        profile.set_preference("browser.download.defaultFolder", download_folder_path)

        return webdriver.Firefox(firefox_profile=profile)

    if browser == 'chrome':
        options = webdriver.ChromeOptions()
        prefs = {"download.default_directory": download_folder_path,
                 "download.prompt_for_download": False,
                 "download.directory_upgrade": True,
                 "safebrowsing.enabled": True}
        options.add_experimental_option("prefs", prefs)
        return webdriver.Chrome(executable_path=driver_path, chrome_options=options)


def jpc_selenium(folder_path, driver_path, url, html_username_attribute, login_username, html_password_attribute,
                 login_password, html_login_button_attribute):
    """
    Download the jpc report from the online portal.
    The steps to download the report are the following:
    1. Login
    2. Click on the 'Stat' button.
    3. Sort the reports two times in order get a descendant sort by date.
    4. Click on the first report starting by 'F62' (refer to the appropriate report).

    :param folder_path: A string indicating the folder that will received the downloaded folder.
    :param url: A string indicating the online portal url.
    :param html_username_attribute: A string indicating the username input id value.
    :param login_username: A string indicating the username of the account.
    :param html_password_attribute: A string indicating the password input id value.
    :param login_password: A string indicating the password of the account.
    :param html_login_button_attribute: A string indicating the submit button input id value.
    """
    driver = set_web_driver_profile_and_preferences(folder_path, driver_path)

    try:
        driver.get(url)
        time.sleep(10)

        driver.find_element_by_id(html_username_attribute).send_keys(login_username)
        time.sleep(5)
        driver.find_element_by_id(html_password_attribute).send_keys(login_password)
        time.sleep(5)
        driver.find_element_by_id(html_login_button_attribute).click()
        time.sleep(5)

        driver.find_element_by_xpath("//*[contains(text(), 'Stat')]").click()
        time.sleep(5)

        # In order to sort in descending order, 2 clicks is needed.
        driver.find_element_by_xpath("//*[contains(text(), 'Date')]").click()
        time.sleep(5)
        driver.find_element_by_xpath("//*[contains(text(), 'Date')]").click()
        time.sleep(5)

        # Only the report that start with "F62" the one that need to be fetched.
        # Since the reports have previously been sorted (descending) by date, the first report with "F62" is the right one.
        for i in driver.find_elements_by_xpath(".//td[@class='file']"):
            if "F62" in i.text:
                driver.find_element_by_xpath("//*[contains(text(), '{0}')]".format(i.text)).click()
                break
        time.sleep(10)

        driver.quit()
    except Exception as e:
        print(e)
    finally:
        driver.quit()


def uni_selenium(folder_path, driver_path, url, html_username_attribute, login_username, html_password_attribute,
                 login_password, html_login_button_attribute):
    """
    Download the uni report from the online portal.
    The steps to download the report are the following:
    1. Login
    2. Click on the 'Données P.O.S.' tab on the left panel.
    3. Click on the link that refers to the excel export.

    :param folder_path: A string indicating the folder that will received the downloaded folder.
    :param url: A string indicating the online portal url.
    :param html_username_attribute: A string indicating the username input name value.
    :param login_username: A string indicating the username of the account.
    :param html_password_attribute: A string indicating the password input name value.
    :param login_password: A string indicating the password of the account.
    :param html_login_button_attribute: A string indicating the submit button input name value.
    """
    driver = set_web_driver_profile_and_preferences(folder_path, driver_path)
    try:
        driver.get(url)
        time.sleep(10)

        driver.find_element_by_name(html_username_attribute).send_keys(login_username)
        time.sleep(5)
        driver.find_element_by_name(html_password_attribute).send_keys(login_password)
        time.sleep(5)
        driver.find_element_by_name(html_login_button_attribute).click()
        time.sleep(10)

        driver.switch_to.frame("MenuGauche")
        driver.find_element_by_xpath("//*[contains(text(), '{0}')]".format("Données P.O.S.")).click()
        time.sleep(5)
        driver.switch_to.default_content()

        driver.switch_to.frame("Page")

        driver.find_element_by_xpath("//a[contains(@href, '.xls')]").click()
        driver.switch_to.default_content()
        time.sleep(10)

        driver.quit()
    except Exception as e:
        print(e)
    finally:
        driver.quit()



def wal_selenium(folder_path, driver_path, url, html_username_attribute, login_username, html_password_attribute,
                 login_password, html_login_button_attribute):
    """
   Download the wal report from the online portal.
   The steps to download the report are the following:
   1. Login
   2. Go the the page that shows all the walmart app.
   3. Search for the 'Decision Support' app.
   4. Click on the save report tab (top right).
   5. Click on the report folder named GPSQLServer.
   6. Right click on the report named 'GPSQLServerPOSReport'.
   7. Select 'submit' from the drop down menu.
   8. Once the new window pops, extract the 'Job Id' from the text.
   9. Go back the home page.
   10. Refresh the page until the report request status with the right 'Job Id' is equal to 'Done'.

    :param folder_path: A string indicating the folder that will received the downloaded folder.
    :param url: A string indicating the online portal url.
    :param html_username_attribute: A string indicating the username input id value.
    :param login_username: A string indicating the username of the account.
    :param html_password_attribute: A string indicating the password input id value.
    :param login_password: A string indicating the password of the account.
    :param html_login_button_attribute: A string indicating the submit button input id value.
   """
    driver = set_web_driver_profile_and_preferences(folder_path, driver_path)

    try:
        driver.get(url)
        time.sleep(10)

        driver.find_element_by_id(html_username_attribute).send_keys(login_username)
        time.sleep(5)
        driver.find_element_by_id(html_password_attribute).send_keys(login_password)
        time.sleep(5)
        driver.find_element_by_id(html_login_button_attribute).click()
        time.sleep(10)

        driver.find_element_by_id("menuAppsLink").click()
        time.sleep(5)

        driver.find_element_by_id("txtSitemapSearch").send_keys("Decision Support")
        time.sleep(5)
        driver.find_element_by_id("filterSearchIcon").click()
        time.sleep(5)
        driver.find_element_by_partial_link_text("Decision Support").click()
        time.sleep(5)

        driver.find_element_by_xpath("//*[@title='{0}']".format("My Saved Reports")).click()
        time.sleep(5)

        driver.switch_to.frame("ifrContent")
        driver.find_element_by_id("IMG27289255").click()
        time.sleep(5)
        ActionChains(driver).context_click(driver.find_element_by_xpath("//span[text()='{0}']".format("GPSQLServerPOSReport"))).perform()
        time.sleep(5)
        driver.find_element_by_xpath("//*[contains(@onclick, '{0}')]".format("submitSaved(event)")).click()
        time.sleep(5)
        driver.switch_to.default_content()

        driver.switch_to.window(driver.window_handles[1])
        time.sleep(5)
        submitReportConfirmationText = driver.find_element_by_xpath("//*[@class='dialogText']").text
        # Extract the digit in the text and convert it to string.
        walmartReportJobId = str([int(s) for s in str.split(submitReportConfirmationText) if s.isdigit()][0])
        time.sleep(5)

        driver.switch_to.window(driver.window_handles[0])
        driver.find_element_by_xpath("//a[@href='/']").click()

        valid = False
        counter = 0
        while not valid:
            # Go threw the report and request table status and target the one corresponding to the walmartReportJobId.
            rowsInReportsAndRequestStatusTable = driver.find_elements_by_xpath("//div[@class='row-fluid statusResultsText']")
            specificJobIdRowInReportsAndRequestStatusTable = [i for i in rowsInReportsAndRequestStatusTable if walmartReportJobId in i.text]

            if "Done" in specificJobIdRowInReportsAndRequestStatusTable[0].text:
                print("The report is ready to download.")
                driver.find_element_by_xpath("//i[contains(@id, '{0}')]".format(walmartReportJobId)).click()
                valid = True

            else:
                if counter >= 500:
                    raise AirflowException("Wal-Mart report refresh timeout.")
                else:
                    print("Waiting 25 seconds before trying again.")
                    time.sleep(25)
                    counter += 1
                    driver.find_element_by_xpath("//a[@href='/']").click()

        time.sleep(15)
        driver.quit()
    except Exception as e:
        print(e)
    finally:
        driver.quit()


def shp_selenium(folder_path, driver_path, url, html_username_attribute, login_username, html_password_attribute,
                 login_password, html_login_button_attribute):
    """
    Download the shp report from the online portal.
    The steps to download the report are the following:
    1. Login
    2. Click on the 'Data Portal' tab in the left panel.
    3. Select the 'SDM Vendor Data Portal_Promo_GENACOL'.
    4. Select the saved bookmark named 'POS_REPORT' (provide the report format).
    5. Select the last 12 week (provide the latest week).
    6. Active the 'Code' by clicking on the button in the top right.
    7. Select the report 'By Week'.
    8. Click on the 'Send to Excel' icon.

    :param folder_path: A string indicating the folder that will received the downloaded folder.
    :param url: A string indicating the online portal url.
    :param html_username_attribute: A string indicating the username input id value.
    :param login_username: A string indicating the username of the account.
    :param html_password_attribute: A string indicating the password input id value.
    :param login_password: A string indicating the password of the account.
    :param html_login_button_attribute: A string contained in the submit button text xpath.
    """
    driver = set_web_driver_profile_and_preferences(folder_path, driver_path)

    try:
        driver.get(url)
        time.sleep(10)

        driver.find_element_by_id(html_username_attribute).send_keys(login_username)
        time.sleep(5)
        driver.find_element_by_id(html_password_attribute).send_keys(login_password)
        time.sleep(5)
        driver.find_element_by_xpath("//button[@class='{0}']".format(html_login_button_attribute)).click()
        time.sleep(5)

        driver.find_element_by_xpath("//a[contains(text(), '{0}')]".format("Data Portal")).click()
        time.sleep(5)

        driver.find_element_by_xpath("//*[@title='{0}']".format("SDM Vendor Data Portal_Promo_GENACOL")).click()
        time.sleep(10)

        new_window = driver.window_handles[1]
        driver.switch_to.window(new_window)
        time.sleep(5)

        driver.find_element_by_xpath("//option[text()='{0}']".format("POS_REPORT")).click()
        time.sleep(5)

        driver.find_element_by_xpath("//td[text()='{0}']".format("L12W")).click()
        time.sleep(5)

        driver.find_element_by_id("19").click()
        time.sleep(5)

        driver.find_element_by_id("25").click()
        time.sleep(5)

        driver.find_element_by_xpath("//*[@title='{0}']".format("Send to Excel")).click()
        time.sleep(10)

        driver.quit()
    except Exception as e:
        print(e)
    finally:
        driver.quit()


def shp_stores_selenium(folder_path, driver_path, url, html_username_attribute, login_username, html_password_attribute,
                        login_password, html_login_button_attribute):
    """
    Download the shp store report from the online portal.
    The steps to download the report are the following:
    1. Login
    2. Click on the 'Data Portal' tab in the left panel.
    3. Select the 'SDM Vendor Data Portal_Promo_GENACOL'.
    4. Select 'Data file'
    5. Select the latest 'yearweek' value.
    6. Click on 'Site Lookup'.
    7. Click on the 'Send to Excel' icon.

    :param folder_path: A string indicating the folder that will received the downloaded folder.
    :param url: A string indicating the online portal url.
    :param html_username_attribute: A string indicating the username input id value.
    :param login_username: A string indicating the username of the account.
    :param html_password_attribute: A string indicating the password input id value.
    :param login_password: A string indicating the password of the account.
    :param html_login_button_attribute: A string contained in the submit button text xpath.
    """
    driver = set_web_driver_profile_and_preferences(download_folder_path=folder_path, driver_path=driver_path)

    try:
        driver.get(url)
        time.sleep(10)

        driver.find_element_by_id(html_username_attribute).send_keys(login_username)
        time.sleep(5)
        driver.find_element_by_id(html_password_attribute).send_keys(login_password)
        time.sleep(5)
        driver.find_element_by_xpath("//button[@class='{0}']".format(html_login_button_attribute)).click()
        time.sleep(5)

        driver.find_element_by_xpath("//a[contains(text(), '{0}')]".format("Data Portal")).click()
        time.sleep(5)

        driver.find_element_by_xpath("//*[@title='{0}']".format("SDM Vendor Data Portal_Promo_GENACOL")).click()
        time.sleep(10)

        new_window = driver.window_handles[1]
        driver.switch_to.window(new_window)
        time.sleep(5)

        driver.find_element_by_xpath('//*[@id="32"]/div[2]/div[1]/div[1]/div[5]/div/div[4]').click()
        time.sleep(5)

        yearweek_list = driver.find_elements_by_xpath("//*[@class='{0}']".format("QvListbox"))[0].text
        print("DEBUG: Html element with date as text %s ... %s." % (str(yearweek_list[:6]), str(yearweek_list[-6:])))

        most_recent_yearweek = yearweek_list.split()[-1]
        print("DEBUG: Last date = {0}.".format(most_recent_yearweek))

        driver.find_element_by_xpath("//*[@title='{0}']".format(most_recent_yearweek)).click()
        time.sleep(5)

        driver.find_element_by_xpath("//*[contains(text(), '{0}')]".format("Site Lookup")).click()
        time.sleep(10)

        driver.find_elements_by_xpath("//*[@title='{0}']".format("Send to Excel"))[1].click()
        time.sleep(10)

        driver.quit()
    except Exception as e:
        print(e)
    finally:
        driver.quit()


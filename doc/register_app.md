# Register and use your own Application ID

For privacy and security concern, you should register your own Application ID
and use it in `onedrive-fuse`.

Steps:

1. Login ANY of your Microsoft accounts (it doesn't need to be the account of your OneDrive),
   goto [`App Registritions` page][app_registrition]
   and click the button `New registration` on the top-left.

   ![App Registrations page](./img/app_registrations.png)

2. Fill the form.
   - `Name` can be randomly chosen.
   - `Supported account types` should be `Personal Microsoft accounts only`
   - `Redirect URI` should be `Public client/native (mobile & desktop)` (chosen from the left list),
     and URL `https://login.microsoftonline.com/common/oauth2/nativeclient` (the right input box).
   - Click `Register`

   ![Register form](./img/register_form.png)

3. In the next page, you will see the information of your registered application.
   The UUID-format `Application (client) ID` is just what we need.
   Copy and save it for the later use.

   ![App information page](./img/app_info.png)

   This Client ID is not really a secret, since it just identify the application.
   You still need further login to your OneDrive account to get the access token.

[app_registrition]: https://portal.azure.com/#blade/Microsoft_AAD_RegisteredApps/ApplicationsListBlade

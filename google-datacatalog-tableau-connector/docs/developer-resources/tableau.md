# Tableau

Ready-to-use Tableau environments with production-level metadata are helpful
resources for developers working on new features to the connector and running
system tests. In this guide you will find information on how to set up them.

## Tableau Online

Tableau Online is an analytics platform fully hosted in the cloud. Developers
can quickly set up a Tableau Site after enrolling the [Tableau Developer
Program](https://www.tableau.com/developer).

Click **SIGN UP FOR THE TABLEAU DEVELOPER PROGRAM**. Once you have signed up
you will receive an e-mail with subject _Tableau Online Developer - Activate
your Site_.

In the e-mail contents, click **Activate My Developer Site**. You will
receive another e-mail with subject _You've Successfully Created Your Site_,
and you are all set to use a brand-new Tableau Online development site.

## Tableau Server

It's possible to set up a Tableau Server on premisses or [public
clouds](https://help.tableau.com/current/server-linux/en-us/ts_tableau_server_cloud_overview.htm).
There is a 14-day trial period for those who don't have a licence key. The
connector development team has played with the Tableau Server for Linux in
Google Cloud Platform.

The official instructions to deploy it can be found
[here](https://help.tableau.com/current/server-linux/en-us/ts_gcp_single_server.htm).
In the below lines we describe some quirks to expedite the setup process
(in addition to the steps from the official guide, using their numbers as a
reference):

- Step 0: Set up a Firewall rule
  - On the Google Cloud Platform dashboard, in the navigation pane, under
    **Networking / VPC network**, click **Firewall**.
  - Create an ingress rule that allows TCP traffic on port `8850` from your
    local IP address / network. Add the `tableau-server` target tag to this
    rule. It will be useful later, when you need to set up the Tableau Services
    Manager.

- Step 1: Set up a Google Compute Engine VM
  - **Machine type**: a `e2-standard-8 (8 vCPUs, 32 GB memory)` machine should
    be enough to run a development/demo server;
  - **Boot disk**: a `40 GB` standard persistence disk should be enough to run
    a development/demo server;
  - **Network tags**: add the `tableau-server` network tag to the VM (see step
    0 for details).  

- Step 2: Connect to your Google Compute Engine VM
  - If you connected to the VM through SSH without using a login and password,
    you will need to set up a password for your user. This password will be
    required later to sign in to Tableau Services Manager. Use
    `sudo passwd $USER` to do so. Log off and log on again to the terminal
    before you configure Tableau Server.
    
- Step 3: Install Tableau Server on your Google Compute Engine VM
  - **Download the installer directly**: if you are using the command line,
    a tool such as `wget` will be useful to download the installer. E.g.
    `wget https://downloads.tableau.com/tssoftware/tableau-server-<version>_amd64.deb`.
  - Run the Tableau Server installer on the VM:
    ```shell script
    sudo apt-get update
    sudo apt-get upgrade
    sudo apt-get -y install gdebi-core
    sudo gdebi -n tableau-server-<version>_amd64.deb
    ```
  - The installer will provide you instructions on how to initialize the
    Tableau Services Manager at the end of the installation process. Something
    like:
    ```shell script
    sudo /opt/tableau/tableau_server/packages/scripts.<version_code>/initialize-tsm \
      --accepteula
    ```
  - It is now time to activate and register the Tableau Server: it can be done
    both from the UI and the CLI, as described
    [here]((https://help.tableau.com/current/server-linux/en-us/activate.htm)).
    The firewall rule created in the **Step 0** allows us to access the UI in
    `https://<external-ip-of-the-vm>:8850`. When prompted to authenticate, you
    can log in using your Linux username, and the password set in the
    **Step 2**.
  - **UI only**:
    - After logging in, provide your Product Key or click **Start Tableau
    Server Trial** if you do not have one.
    - In the next screen, fulfill the required fields and click **Register**.
    - Setup screen: set _Identity Store_ to `Local`, decide whether allow
      sending usage data to Tableau or not, and leave the other fields with
      their default values.
  - Wait for the setup process to finish, it will take a few minutes.
  - Back to the command line, run the below commands. Replace
    `<new-admin-username>` and `<new-admin-password>` with the new credentials
    to create the initial Tableau Server user (it is not the same user that
    authenticated in the Tableau Services Manager).
    ```shell script
    tsm pending-changes apply
    tabcmd initialuser \
      --server 'localhost:80' \
      --username '<new-admin-username>' \
      --password '<new-admin-password>'
    ```
  - Finally, run the bellow command to enable the Metadata API:
    ```shell script
    tsm maintenance metadata-services enable
    ```

Once deployed, you can access the Tableau Server from the browser
(`http://<external-ip-of-the-vm>`), [Postman](./postman.md), or by running the
connector.

## Public Workbooks

After setting up a Tableau Online site or Tableau Server, you can either create
your own Workbooks or import public ones. In the below pages you can find free
downloadable Workbooks and start adding content to your sites:
- [Tableau Public](https://public.tableau.com): a free platform to publicly
  share and explore data visualizations online.
- [Tableau Dashboard Starters](https://www.tableau.com/products/dashboard-starters-downloads)
  although designed to connect to common enterprise applications and analyse
  actual data, they come with sample data that fit very well for testing and
  demonstration purposes.

---

Back to the [Developer Resources index](..)

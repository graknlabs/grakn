---
title: Google Cloud Deployment
keywords: cloud, deployment, google
tags: [getting-started, deployment, cloud]
summary: "Deploying Grakn on Google Cloud"
sidebar: documentation_sidebar
permalink: /docs/cloud-deployment/gc-deployment
folder: docs
---

# Google Cloud Launcher

## Deployment
We shall begin with deployment of a Grakn cluster. The procedure is straight-forward and takes advantage of the
[Cloud Launcher](https://console.cloud.google.com/launcher/details/grakn-public/grakn-kgms-premium):

![](/images/gc-solution-listing.png)

To start deployment, click on the `Launch on Compute Engine` button which will take you to the configuration screen

![](/images/gc-deployment-options.png)

Feel free to adjust the settings to your needs. When satisfied with the configuration press `Deploy`

![](/images/gc-deployment-pending.png)

That is all! Your cluster deployment is now pending.

When the deployment is complete you should be able to see the post-deployment screen:

![](/images/gc-deployment-complete.png)

Once the vms are deployed, please allow some time for cluster to fully bootup and synchronise. 2 minutes per node in a cluster is a reasonable rule of thumb for full bootup time.

## Accessing Grakn
There are various ways to access Grakn in the cloud. Here we will address the most common usage patterns.

### Using Grakn gRPC client

To enable gRPC communication, traffic on TCP port 48555 needs to be allowed. It is enabled by default. If you chose otherwise for your deployment, a suitable firewall rule can be created if needed by executing the command in red circle in your terminal:

![](/images/gc-grpc-firewall-command.png)

### Logging in into a node
You may require a more direct interaction with the database. You need to log into a node to achieve that.
To do so go back to the Google console and follow the red arrow as shown below to start an ssh session using the `Open in browser window` option.

![](/images/gc-ssh-button.png)

Once logged in, a variety of interactions are possible through `grakn` and `graql` terminals.

#### Cluster health check
To check cluster health, execute the `grakn cluster status` command. The output shall look like this:

![](/images/gc-cluster-health.png)

#### Accessing the Graql console
To access the Graql console, a user password is required. You can see it in the Google console screen in the red circle:

![](/images/gc-user-password.png)

To log into the Graql console, simply type `graql console`. After entering the user credentials (user: grakn, password: the one from the Google console) you are free to interact with Grakn via the Graql terminal. Successful login attempt shall look like this:

![](/images/gc-graql-console.png)

A summary of available commands can be found [here](http://dev.grakn.ai/docs/get-started/graql-console).

#### Accessing the Grakn console
The Grakn console can be accessed similarly to Graql console by typing `grakn console start` and providing the user credentials. Successful login will look like this:

![](/images/gc-grakn-console.png)

Provided you log in as user with `admin` privileges, Grakn console allows you to perform the following actions:

* create a new user:

`CREATE USER username WITH PASSWORD userpassword WITH ROLE admin`

* update an existing user's password

`UPDATE username WITH PASSWORD newpassword`

* retrieve all of the users present:

`LIST USERS`

* retrieve a user:

`GET USER username`

* delete an existing user:

`DELETE USER username`


## Next Steps

Deploying Grakn KGMS on Google Cloud comes with our Enterprise Support. Please contact [Support](support@grakn.ai) with your Support ID so we can create your account on our [Support Platform](https://work.grakn.ai/helpdesk). 

If you want to learn more about Grakn KGMS, the [Grakn Academy](https://dev.grakn.ai/academy/) is a good place to start.

To learn more about running Grakn KGMS in the cloud, take a look at the [best practices guide](https://dev.grakn.ai/docs/cloud-deployment/best-practices)
and [post deployment steps](https://dev.grakn.ai/docs/cloud-deployment/post-deployment).

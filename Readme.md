# Flight Loads Proof of Concept
The idea for this repo is to build  a first approach to what the predicting model for the flight loads will be.
Code in this repo is hacky and might not always work since this is not meant to be a production ready product, but a sandbox for devs to try out stuff

## Build the project
At the moment of creating this document, there are a couple of requirements described in the `requirements.txt` file, these will be installed at build time.

For working with this project, you'll need to connect to either a local instance of the FLIGHTLOADSDATA database or use Beta or Prod schemas (can be found in the Flights RDS under the name of FLIGHTLOADSDATA_<env>).
If you're going to use a local database, the `db` service included in the `docker-compose.yml` might be useful, if you're going with the existing schemas, you'll need to rename the `.env.beta` file to `.env` and add your username and password to it. Talk to the DBA if you need access.

After the environment step is prepared, run `docker compose up` and the image should build and keep a running container.

Connect to the container anyway you want (through vscode or manually) and you can start running the project.

Start with `python test.py help` for a help menu
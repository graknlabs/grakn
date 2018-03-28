# GRAKN.AI Documentation

## TL;DR

**How to update and build the docs**

* Install `bundler` and `rake` (one time only task)
* Pull down documentation repo
* Make changes to markdown
* `node ./deploy-server.js` in terminal and browse to [http://127.0.0.1:3003](http://127.0.0.1:3003) (use `rake serve` if `node` is not installed on your machine)
* If you need to make some extra changes to the markdown after you have the server running, just change the files and save them. `rake` will pick up the changes and rebuild the docs so you don't need to do anything.
* When you're happy, push to the docs repo to `stable` branch
* Once the changes have been merged to stable, go to `/docs` and run `./deploy.sh` script (you can run this script from whatever brunch on your machine)

## Dependencies

You need to install the following dependencies to be able to build HTML pages in the documentation repository. **NOTE:** this is *not* necessary for updating the documentation itself.

1. Bundle; you will need to install `bundler` through your package manager of choice.

    **Arch Linux**
    ```
    $ yaourt -S ruby-bundler
    ```

    **OSX**
    ```
    $ brew install bundler
    ```

    **Ruby Gems (generic)**
    ```
    $ gem install bundler
    ```

2. Rake; this is used to automate the rest of the site building process.
    ```
    $ gem install rake
    ```

    With `rake` installed you can now install all other dependencies:
    ```
    $ rake dependencies
    ```

3. NPM Modules; These are used to run the deployment server tests.
    ```
    $ yarn install
    ```

## Building

You can generate the documentation HTML by running the following in the repository top level.
```
$ rake build
      Generating...
                    done in 1.503 seconds.
 Auto-regeneration: disabled. Use --watch to enable.
$
```

This will build the documentation site in `_jekyll` and create a symlink `_site` in the repository top level directory which will contain all the generated content.

## Cleaning

Clean by running the following command in the repository top level:
```
$ rake clean
```

This will remove all generated files.

## Serving

You can also build and server the generated HTML files in one command. A web
server will be started listening on `localhost` (127.0.0.1) on port 4005

```
$ rake serve
    Server address: http://127.0.0.1:4005/
  Server running... press ctrl-c to stop.
$
```

You can now view the documentation by navigating your web browser to `http://127.0.0.1:4005/`

## Deployment & Final Testing
After you are done making changes, run the deployment server with ```node ./deploy-server.js``` to test how everything is going to look on releasing.
You can access the site on `http://127.0.0.1:3003/`

If everything works, create a new PR against grakn [stable branch](https://github.com/graknlabs/grakn/tree/stable).
Once the PR has been merged run the `deploy.sh` script inside `/docs`.
The script deploys the application to our heroku server. Make sure you have the correct git credentials.

> If deploying from some other branch make sure to edit `deploy.sh` file to branch off accordingly.

## Tests

There are a few tests we run against docs:

- `html-proofer`
- `GraqlDocsTest`
- `JavaDocsTest`

`html-proofer` can be executed with `rake test`. It will check all the links in the docs to make sure they actually go
somewhere.

`GraqlDocsTest` and `JavaDocsTest` will test the Graql and Java code blocks respectively. Blocks are identified by
whether they begin with `graql` or `java`. Each page is tested on its own by executing the code blocks sequentially.

By default, the code blocks are executed against the genealogy knowledge graph. If you want to use a different knowledge
base, then add e.g.
```
KB: pokemon
```
to the header of the markdown file. The valid knowledge graphs can be found in `DocTestUtils`.

Java code blocks are actually tested with Groovy (because it is an interpreted language). There are some differences
between Java and Groovy syntax, so we recommend writing code that is valid in both languages.

If a code block should not be executed (e.g. because it is deliberately invalid or does something dangerous), then mark
it `graq-test-ignore` or `java-test-ignore` instead of `graql` or `java`.

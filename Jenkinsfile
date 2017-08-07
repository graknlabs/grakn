node {
  //Everything is wrapped in a try catch so we can handle any test failures
  //If one test fails then all the others will stop. I.e. we fail fast
  try {
    def workspace = pwd()
    //Always wrap each test block in a timeout
    //This first block sets up engine within 15 minutes
    timeout(15) {
      stage('Build Grakn') {//Stages allow you to organise and group things within Jenkins
        sh 'npm config set registry http://registry.npmjs.org/'
        checkout scm
        sh 'if [ -d maven ] ;  then rm -rf maven ; fi'
        sh "mvn versions:set -DnewVersion=${env.BRANCH_NAME} -DgenerateBackupPoms=false"
        sh 'mvn clean package -DskipTests -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT'
        archiveArtifacts artifacts: "grakn-dist/target/grakn-dist*.tar.gz"
      }
      stage('Init Grakn') {
        sh 'if [ -d grakn-package ] ;  then rm -rf grakn-package ; fi'
        sh 'mkdir grakn-package'
        sh 'tar -xf grakn-dist/target/grakn-dist*.tar.gz --strip=1 -C grakn-package'
        sh 'grakn-package/bin/grakn.sh start'
      }
      stage('Test Connection') {
        sh 'grakn-package/bin/graql.sh -e "match \\\$x;"' //Sanity check query. I.e. is everything working?
      }
    }
    //Sets up environmental variables which can be shared between multiple tests
    withEnv(['VALIDATION_DATA=' + workspace + '/generate-SNB/readwrite_neo4j--validation_set.tar.gz',
             'CSV_DATA=' + workspace + '/generate-SNB/social_network',
             'KEYSPACE=snb',
             'ENGINE=localhost:4567',
             'ACTIVE_TASKS=1000',
             'PATH+EXTRA=' + workspace + '/grakn-package/bin',
             'LDBC_DRIVER=' + workspace + '/ldbc-driver/target/jeeves-0.3-SNAPSHOT.jar',
             'LDBC_CONNECTOR=' + workspace + '/benchmarking/snb-interactive-grakn/target/snb-interactive-grakn-stable-jar-with-dependencies.jar',
             'LDBC_VALIDATION_CONFIG=readwrite_grakn--ldbc_driver_config--db_validation.properties']) {
      timeout(90) {
        dir('generate-SNB') {
          stage('Load Validation Data') {
            sh 'wget https://github.com/ldbc/ldbc_snb_interactive_validation/raw/master/neo4j/readwrite_neo4j--validation_set.tar.gz'
            sh '../grakn-test/test-snb/src/generate-SNB/load-SNB.sh arch validate'
          }
        }
        stage('Measure Size') {
          sh 'nodetool flush'
          sh 'du -hd 0 grakn-package/db/cassandra/data'
        }
      }
//        timeout(360) {
//          dir('validate-SNB') {
//            stage(buildBranch+' Validate Graph') {
//              sh './validate.sh'
//            }
//          }
//        }
    }
    slackSend channel: "#github", message: "Periodic Build Success on ${env.BRANCH_NAME}: ${env.BUILD_NUMBER} (<${env.BUILD_URL}flowGraphTable/|Open>)"
  } catch (error) {
    slackSend channel: "#github", message: "Periodic Build Failed on ${env.BRANCH_NAME}: ${env.BUILD_NUMBER} (<${env.BUILD_URL}flowGraphTable/|Open>)"
    throw error
  } finally { // Tears down test environment
    timeout(5) {
      stage('Tear Down Grakn') {
        sh 'if [ -d maven ] ;  then rm -rf maven ; fi'
        archiveArtifacts artifacts: 'grakn-package/logs/grakn.log'
        sh 'grakn-package/bin/grakn.sh stop'
        sh 'if [ -d grakn-package ] ;  then rm -rf grakn-package ; fi'
      }
    }
  }
}
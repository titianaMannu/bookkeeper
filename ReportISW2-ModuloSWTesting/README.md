
# Usage

    JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64" mvn clean verify

Nella directory: target/site/jacoco-aggregate si trova il report di Jacoco 

Per generare pit-report

     cd bookkeeper-server/ &&  mvn org.pitest:pitest-maven:mutationCoverage

Per generare coverage con ba-dua esiste un profilo apposito nel pom.xml in bookkeeper-server/.

Il profilo non è attivo di default.
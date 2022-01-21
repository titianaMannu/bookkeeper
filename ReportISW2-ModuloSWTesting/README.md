
# Usage

    mvn clean verify

Per generare pit-report

     cd bookkeeper-server/ &&  mvn org.pitest:pitest-maven:mutationCoverage

Per generare coverage con ba-dua esiste un profilo apposito nel pom.xml in bookkeeper-server/.

Il profilo non è attivo di default.
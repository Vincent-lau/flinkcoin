FROM maven:3.6-jdk-13-alpine as maven
WORKDIR /usr/src/app
COPY ./pom.xml ./pom.xml
COPY ./src ./src

RUN mvn dependency:go-offline -B
RUN mvn package

FROM eclipse-temurin:11
WORKDIR /usr/src/app
COPY --from=maven /usr/src/app/target/flinkcoin-0.1.jar ./flinkcoin.jar

CMD ["java", "-jar", "./flinkcoin.jar"]

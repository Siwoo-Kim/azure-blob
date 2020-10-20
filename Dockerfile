FROM openjdk:8-jdk-alpine
RUN addgroup -S siwoo && adduser -S siwoo -G siwoo
USER siwoo:siwoo
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
package com.phact;

public class App {
    public static void main(String[] args){
        String keyspaceTable = args[2];
        String sourceIp= args[0];
        String destIp= args[1];
        //This is how you would migate
        Migrate test = new Migrate(keyspaceTable,sourceIp, destIp);
        //This is how you would read RDD's from both clusters
        Join test2 = new Join(keyspaceTable,sourceIp,destIp);
    }
}

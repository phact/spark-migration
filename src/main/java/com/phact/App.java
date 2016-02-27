package com.phact;

public class App {
    public static void main(String[] args){
        String keyspaceTable = args[2];
        String sourceIp= args[0];
        String destIp= args[1];
        //Migrate test = new Migrate(keyspaceTable,sourceIp, destIp);
        Join test2 = new Join(keyspaceTable,sourceIp,destIp);
    }
}

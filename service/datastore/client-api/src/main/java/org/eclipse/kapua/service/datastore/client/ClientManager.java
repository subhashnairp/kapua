package org.eclipse.kapua.service.datastore.client;

public class ClientManager {

    private static Client instance;

    static {
        String implementation = null; // TODO get from conf
        try {
            instance = (Client) ClientManager.class.getClassLoader().loadClass(implementation).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new InternalError();// TODO use Kapua runtime exception
        }

        /*
         * Class clazz = Class.forName("test.Demo");
         * Demo demo = (Demo) clazz.newInstance();
         */
    }

    public static Client getInstance() {
        return instance;
    }

}

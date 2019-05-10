package com.homeaway.datatools.photon.api.beam;

public interface MessageHandler<T, E> {

    /**
     * This method is called back to when a message is encountered that appears in the correct order.
     *
     * @param message - The message to be processed
     */
    void handleMessage(T message);

    /**
     * This method is called back to when an exception is encountered while reading from Photon.
     *
     * @param exception - An exception that is thrown.
     */
    void handleException(E exception);

    /**
     * This method is called back to when a message is encountered that appears to have been written out of order.
     *
     * @param message - The message to be processed.
     */
    void handleStaleMessage(T message);

}

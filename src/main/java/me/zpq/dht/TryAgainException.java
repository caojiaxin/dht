package me.zpq.dht;

/**
 * @author zpq
 * @date 2019-09-09
 */
public class TryAgainException extends Exception {

    public TryAgainException() {
        super();
    }

    public TryAgainException(String message) {
        super(message);
    }
}

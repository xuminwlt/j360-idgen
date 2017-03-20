package me.j360.idgen.client;

public interface IDPool {
	
    String borrow();

    void giveback(String id);

    void consume(String id);
}

package wjc.kafka;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 16:58
 **/
public class Customer {
    private int    id;
    private String name;

    public Customer(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}

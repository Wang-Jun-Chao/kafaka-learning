package wjc.kafka;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 16:58
 **/
public class Customer {
    private long   id;
    private String name;

    public Customer(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}

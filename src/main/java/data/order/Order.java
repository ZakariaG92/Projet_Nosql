package data.order;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;

public class Order {

    @SerializedName("OrderId")
    public String orderId;

    @SerializedName("PersonId")
    public String personId;

    @SerializedName("OrderDate")
    public String orderDate;

    @SerializedName("TotalPrice")
    public float totalPrice;

    @SerializedName("Orderline")
    Orderline[] orderline ;

}

package data.order;

import com.google.gson.annotations.SerializedName;

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
	Orderline[] orderline;

	public Order() {

	}
}

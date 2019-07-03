package hbaseTest;

public class Contact {

    private String id;
    private String name;
    private String mobilePhone;
    private String fixeNumber ;
    private String adresse;


    public Contact(String id, String name, String mobilePhone, String fixeNumber,String adresse) {
        this.id = id;
        this.name = name;
        this.mobilePhone = mobilePhone;
        this.fixeNumber = fixeNumber;
        this.adresse= adresse;
    }
}

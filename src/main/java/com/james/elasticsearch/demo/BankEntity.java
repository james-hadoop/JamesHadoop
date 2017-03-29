package com.james.elasticsearch.demo;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BankEntity {
    @JsonProperty(value = "account_number")
    private Integer account_number;

    @JsonProperty(value = "balance")
    private Integer balance;

    @JsonProperty(value = "firstname")
    private String firstname;

    @JsonProperty(value = "lastname")
    private String lastname;

    @JsonProperty(value = "age")
    private Integer age;

    @JsonProperty(value = "gender")
    private String gender;

    @JsonProperty(value = "address")
    private String address;

    @JsonProperty(value = "employer")
    private String employer;

    @JsonProperty(value = "email")
    private String email;

    @JsonProperty(value = "city")
    private String city;

    @JsonProperty(value = "state")
    private String state;

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    public Integer getAccount_number() {
        return account_number;
    }

    public void setAccount_number(Integer account_number) {
        this.account_number = account_number;
    }

    public Integer getBalance() {
        return balance;
    }

    public void setBalance(Integer balance) {
        this.balance = balance;
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getEmployer() {
        return employer;
    }

    public void setEmployer(String employer) {
        this.employer = employer;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}

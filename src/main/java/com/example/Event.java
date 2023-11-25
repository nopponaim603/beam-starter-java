package com.example;

import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Objects;

public class Event implements Serializable {

    private String id;
    private String event;
    private DateTime date;

    public Event(String id, String event, DateTime date) {
        this.id = id;
        this.event = event;
        this.date = date;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public DateTime getDate() {
        return date;
    }

    public void setDate(DateTime date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Event event1 = (Event) o;

        return id.equals(event1.id) &&
                event.equals(event1.event) &&
                date.equals(event1.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, event, date);
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", event='" + event + '\'' +
                ", date=" + date +
                '}';
    }

}

# gnarly_gnu
A demonstration of baggage handling at an airport with processing using Apache Spark

Milestone 1 (November 2023):
Data Generation using Databricks

Milestone 2 (November 2023):
Data batch processing in Delta Lake

Milestone 3 (November 2023):
Online inference for data structure

Milestone 4 (December 2023):
Containerization of services

## Description
One of the top considerations for commercial airline travel is customer
experience, and with regards to customer experience baggage processing
is top-of-mind. Having a low-cost, reliable experience of travel can be a deciding
factor when choosing which carrier to travel with. In this demo project, we will examine
how to best track and understand what's happening during the baggage handling process at
a given airport.

## Databricks Connect  
DBConnect v2 was used in the development of some parts of this project. To get a quick overview
of how this was set up on PyCharm, the documentation can be found [here](https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html).
The [Databricks SDK](https://docs.databricks.com/en/dev-tools/sdk-python.html) is also extensively used.

## Data Structure

* **BagID** is a unique identifier for each bag.
* **Timestamp** is the date and time when the bag was at the recorded location.
* **Location** is a description of where the bag is (e.g., Check-In, Security, Sorting Area, Gate, On Plane).
* **ConveyorID** is an identifier for which conveyor belt the bag is on, if applicable.
* **FlightID** is the identifier for the flight the bag is supposed to be on.
* **PassengerName** is the name of the passenger who checked the bag.
* **Destination** is the final destination airport for the bag.
* **Status** is the current status of the bag (e.g., Checked In, In Transit, Loaded).
* **Weight** is the weight of the bag.

```
+------------+---------------------+--------------+-------------+------------+----------------+------------------+------------+------------------+
| BagID      | Timestamp           | Location     | ConveyorID  | FlightID   | PassengerName  | Destination      | Status     | Weight          |
+------------+---------------------+--------------+-------------+------------+----------------+------------------+------------+------------------+
| 1234567890 | 2023-10-27 08:00:00 | Check-In     |             | AA123      | John Doe       | JFK              | Checked In | 23.5kg         |
| 1234567890 | 2023-10-27 08:15:00 | Security     | C1          | AA123      | John Doe       | JFK              | In Transit | 23.5kg         |
| 1234567890 | 2023-10-27 08:30:00 | Sorting Area | S5          | AA123      | John Doe       | JFK              | In Transit | 23.5kg         |
| 1234567890 | 2023-10-27 08:45:00 | Gate A5      | G3          | AA123      | John Doe       | JFK              | In Transit | 23.5kg         |
| 1234567890 | 2023-10-27 09:00:00 | On Plane     |             | AA123      | John Doe       | JFK              | Loaded     | 23.5kg         |
| 2345678901 | 2023-10-27 08:05:00 | Check-In     |             | BA456      | Jane Smith     | LHR              | Checked In | 18.2kg         |
| 2345678901 | 2023-10-27 08:20:00 | Security     | C2          | BA456      | Jane Smith     | LHR              | In Transit | 18.2kg         |
| 2345678901 | 2023-10-27 08:35:00 | Sorting Area | S7          | BA456      | Jane Smith     | LHR              | In Transit | 18.2kg         |
| 2345678901 | 2023-10-27 08:50:00 | Gate B2      | G7          | BA456      | Jane Smith     | LHR              | In Transit | 18.2kg         |
| 2345678901 | 2023-10-27 09:05:00 | On Plane     |             | BA456      | Jane Smith     | LHR              | Loaded     | 18.2kg         |
+------------+---------------------+--------------+-------------+------------+----------------+------------------+------------+------------------+
```
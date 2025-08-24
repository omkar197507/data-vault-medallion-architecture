\# Sample Data Generation Guide



\## Overview

This guide covers generating realistic sample data for the Data Vault Medallion Architecture project.



\## Data Specifications



\### Customers Table

\- \*\*Records\*\*: 1,000 customers

\- \*\*Fields\*\*: customer\_id, first\_name, last\_name, email, phone, address, city, state, zip\_code, dates

\- \*\*Relationships\*\*: Links to orders



\### Products Table  

\- \*\*Records\*\*: 100 products

\- \*\*Fields\*\*: product\_id, product\_name, category, price, cost, dates

\- \*\*Categories\*\*: Electronics, Clothing, Home, Books, Sports



\### Orders Table

\- \*\*Records\*\*: 5,000 orders  

\- \*\*Fields\*\*: order\_id, customer\_id, order\_date, total\_amount, status, created\_date

\- \*\*Statuses\*\*: COMPLETED (70%), PENDING (10%), CANCELLED (10%), PROCESSING (10%)



\### Order Items Table

\- \*\*Records\*\*: ~15,000 order items (1-5 items per order)

\- \*\*Fields\*\*: order\_item\_id, order\_id, product\_id, quantity, unit\_price, total\_price, created\_date



\## Generation Steps



\### 1. Install Dependencies

```bash

pip install -r scripts/data-generation/requirements.txt


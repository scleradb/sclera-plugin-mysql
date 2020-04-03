# Sclera - MySQL Connector

[![Build Status](https://travis-ci.org/scleradb/sclera-plugin-mysql.svg?branch=master)](https://travis-ci.org/scleradb/sclera-plugin-mysql)

Enables Sclera to work with your data stored in [MySQL](http://www.mysql.com).

You just need to link your MySQL database with Sclera, then import the metadata of select tables within the database. All this gets done in a couple of commands -- and enables you to include these tables within your Sclera queries.

*To work with Sclera, MySQL should be configured in the [case-insensitive mode](http://dev.mysql.com/doc/refman/8.0/en/identifier-case-sensitivity.html).*

The connector uses [MySQL Connector/J](http://dev.mysql.com/doc/connector-j/en/index.html), which is automatically downloaded during the installation of this component.

Details on how to link your MySQL source to with Sclera can be found in the [Sclera Database System Connection Reference](https://scleradb.com/docs/setup/dbms/#connecting-to-mysql) document.

The MySQL Connector/J JDBC driver is licensed under the GNU General Public License version 2 with FOSS exception. This connector is licensed under the Apache License version 2.0, which is compatible with the said FOSS exception.

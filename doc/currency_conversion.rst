===================
Currency Conversion
===================

This chapter contains the documentation of the currency conversion features
implemented in the SAP HANA Vora Spark extension library.

Overview
--------

Two forms of currency conversion exists in the SAP HANA Vora Spark extension library:

 - **Standard Currency Conversion**: This conversion method uses a single **rates table**
   to perform the conversion and provides a standard set of options to adapt the conversion
   process. It is available in the open source extension library.

 - **ERP Currency Conversion**: This conversion method is compatible to the ERP conversion
   method implemented in HANA.
   **Note**: This conversion method depends on an external package which is only distributed with
   SAP HANA Vora.

Standard Currency Conversion
----------------------------

Overview
````````

The standard currency conversion is based on a given **rates table**. The schema of the
rates table must be:

::

  ( SOURCE_CUR string,
    TARGET_CUR string,
    REF_DATE string,
    RATE double )

The names of the columns can be arbitrary. Their semantics are:

* ``SOURCE_CUR``: The source currency as a string identifier (e.g., ``USD``)
* ``TARGET_CUR``: The taget currency as a string identifier (e.g., ``EUR``)
* ``REF_DATE``:   A string in the ``YYYY-MM-DD`` format. **NOTE**: This is currently a string to be compatible with the ERP conversion. It will change to be a ``Timestamp`` type after the Q2 Beta release.
* ``RATE``:       The conversion rate in arbitrary precision (normally 5 digits).

A rates table may look like follows:

::

   +------------+------------+------------+---------+
   | SOURCE_CUR | TARGET_CUR | REF_DATE   | RATE    |
   +------------+------------+------------+---------+
   | EUR        | USD        | 2015-01-01 | 1.32113 |
   | EUR        | USD        | 2015-01-02 | 1.30121 |
   | EUR        | USD        | 2015-01-03 | 1.28042 |
   | EUR        | USD        | 2015-01-04 | 1.31763 |
   | USD        | EUR        | 2015-01-01 | 0.89464 |
   | USD        | EUR        | 2015-02-01 | 0.86297 |
   | USD        | GBP        | 2015-01-01 | 0.68960 |
   | USD        | GBP        | 2015-02-01 | 0.70544 |
   +------------+------------+------------+---------+

For our examples, we assume the following *orders table*:

::

   +--------------+----------+--------+------------+
   | TID | USERID | CURRENCY | AMOUNT | ORDERDATE  |
   +--------------+----------+--------+------------+
   | 100 | user1  | USD      | 120.10 | 2014-12-15 |
   | 101 | user1  | USD      | 24.99  | 2015-01-01 |
   | 102 | user5  | EUR      | 24.11  | 2015-01-02 |
   | 103 | user3  | GBP      | 542.00 | 2015-01-02 |
   | 104 | user5  | EUR      | 11.99  | 2015-01-03 |
   | ... | ...    | ...      | ...    | ...        |
   +-----+--------+----------+--------+------------+

The standard **currency conversion** function is defines as follows:

::

   CC( AMOUNT Double,
       SOURCE_CURRENCY String,
       TARGET_CURRENCY String,
       REF_DATE String )


Given the orders table above, the conversion function can be used as follows:

::

   SELECT TID,
          USERID,
          ORDERDATE,
          CC( AMOUNT, CURRENCY, "USD", ORDERDATE )
   FROM ORDERS

* For all arguments of the function the values can either come from a column or can be
  set explicitly in the statement. E.g., the conversion at a fix reference data would be:

::

 SELECT TID,
        USERID,
        ORDERDATE,
        CC( AMOUNT, CURRENCY, "USD", "2015-01-01" )
 FROM ORDERS



Conversion Method
`````````````````

The standard conversion method is defined as follows:

::

   CC(AMOUNT, SOURCE_CURRENCY, TARGET_CURRENCY, REF_DATE) :=
      AMOUNT * RATE_LOOKUP(SOURCE_CURRENCY, TARGET_CURRENCY, REF_DATE)

   RATE_LOOKUP(SOURCE_CURRENCY, TARGET_CURRENCY, DATE) :=
      if REF_DATE <= DATE exists:
        The RATE value where SOURCE_CURRENCY and TARGET_CURRENCY
        match and (TIME - REF_TIME) is the minimum
      else: NULL

For conversions with lacking rates, th result is either ``NULL`` or the
method throws an exception (based on the ``error_handling`` option; see below)

Conversion example:

- For ``TID = 102`` in the orders table the conversion to ``USD`` based on the rates table
  would be: ``24.11 * 1.28042 = 30.8709262``. The result will be rounded based on the currency
  options.
- For ``TID = 100`` in the orders table the conversion to ``USD`` would fail or be ``NULL``
  (depending on the currency options) since there exist no valid entry
  in the rates table.


Options
```````

The standard conversion function can be adapted by setting the options with
the ``spark.sql.currency.basic.*``. An option can be set:

- In the ``spark-defaults.conf`` file
- During startup of Spark by setting the config parameters on the command line
- Within a SparkSQL session by using the ``SET`` statement, e.g.,

::

  SET spark.sql.currency.basic.OPTIONNAME = OPTIONVALUE;

E.g., the rates table can be set by:

::

  SET spark.sql.currency.basic.table = new_rates_table;


The following options are available

=====================  =================================================================================  ===================  ========================
Name                   Description                                                                        Default value        Example values
=====================  =================================================================================  ===================  ========================
``table``              The name of a SparkSQL table with the conversion rates.                            ``RATES``            ``CONVERSION_RATES``
``allow_inverse``      If inverse lookup is allowed, the conversion will try to                           ``false``            ``true``
                       do a regular rate lookup first. In case a matching rate
                       cannot be found, it tries to lookup the inverse rate
                       (SOURCE and TARGET switched during lookup) and performs the conversion
                       using ``AMOUNT / RATE``.
``do_update``          This option triggers an update of currency function before                         ``false``            ``true``
                       the next method call. By this,
                       new values in the rates table are considered in the conversion.
                       The option is automatically set to ``false`` after the update
                       has been performed.
``error_handling``     Specifies how the function should behave in case a conversion rate could           ``fail_on_error``    ``set_to_null``
                       not be found:

                       * ``fail_on_error``: Aborts the Spark job and straces exception
                       * ``set_to_null``: Returns ``NULL`` for that row
                       * ``keep_unconverted``: Uses the input amount
=====================  =================================================================================  ===================  ========================

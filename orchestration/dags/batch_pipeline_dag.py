from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta


# ----------------------------------
# Default Arguments with Error HandlingS
# ----------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


# ----------------------------------
# Helper function to create tasks
# ----------------------------------
def create_databricks_task(task_id, notebook_path):
    """Create a DatabricksSubmitRunOperator task"""
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        tasks=[
            {
                "task_key": f"{task_id}_task",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "run_date": "{{ ds }}",
                        "env": "prod"
                    }
                }
            }
        ],
        do_xcom_push=True,
    )


# ----------------------------------
# DAG Definition
# ----------------------------------
with DAG(
    dag_id="travel_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["databricks", "spark", "lakehouse", "parallel"],
) as dag:

    # =====================================================
    # Task Group 1: S3 to Bronze (All tasks run in parallel)
    # =====================================================
    with TaskGroup("s3_to_bronze") as s3_to_bronze_group:
        
        aircrafts_s3_to_bronze = create_databricks_task(
            "aircrafts_s3_to_bronze",
            "/Workspace/Shared/TravelAgent_Test/S3_To_DeltaLake_Bronze/Aircrafts_From_S3_To_DeltaLake_Bronze"
        )
        
        airlines_s3_to_bronze = create_databricks_task(
            "airlines_s3_to_bronze",
            "/Workspace/Shared/TravelAgent_Test/S3_To_DeltaLake_Bronze/Airlines_From_S3_To_DeltaLake_Bronze"
        )
        
        airports_s3_to_bronze = create_databricks_task(
            "airports_s3_to_bronze",
            "/Workspace/Shared/TravelAgent_Test/S3_To_DeltaLake_Bronze/Airports_From_S3_To_DeltaLake_Bronze"
        )
        
        customers_s3_to_bronze = create_databricks_task(
            "customers_s3_to_bronze",
            "/Workspace/Shared/TravelAgent_Test/S3_To_DeltaLake_Bronze/Customers_From_S3_To_DeltaLake_Bronze"
        )
        
        flight_booking_s3_to_bronze = create_databricks_task(
            "flight_booking_s3_to_bronze",
            "/Workspace/Shared/TravelAgent_Test/S3_To_DeltaLake_Bronze/Flight_Booking_From_S3_To_DeltaLake_Bronze"
        )
        
        hotel_bookings_s3_to_bronze = create_databricks_task(
            "hotel_bookings_s3_to_bronze",
            "/Workspace/Shared/TravelAgent_Test/S3_To_DeltaLake_Bronze/Hotel_Bookings_From_S3_To_DeltaLake_Bronze"
        )
        
        hotels_s3_to_bronze = create_databricks_task(
            "hotels_s3_to_bronze",
            "/Workspace/Shared/TravelAgent_Test/S3_To_DeltaLake_Bronze/Hotels_From_S3_To_DeltaLake_Bronze"
        )

    # =====================================================
    # Task Group 2: Validation (All tasks run in parallel)
    # =====================================================
    with TaskGroup("bronze_validation") as validation_group:
        
        aircraft_validation = create_databricks_task(
            "aircraft_validation",
            "/Workspace/Shared/TravelAgent_Test/Validation/Aircraft_Validation_GE"
        )
        
        airline_validation = create_databricks_task(
            "airline_validation",
            "/Workspace/Shared/TravelAgent_Test/Validation/Airline_Validation_GE"
        )
        
        airport_validation = create_databricks_task(
            "airport_validation",
            "/Workspace/Shared/TravelAgent_Test/Validation/Airport_Validation_GE"
        )
        
        customer_validation = create_databricks_task(
            "customer_validation",
            "/Workspace/Shared/TravelAgent_Test/Validation/Customer_Validation_GE"
        )
        
        flight_bookings_validation = create_databricks_task(
            "flight_bookings_validation",
            "/Workspace/Shared/TravelAgent_Test/Validation/Flight_Bookings_Validation_GE"
        )
        
        hotel_bookings_validation = create_databricks_task(
            "hotel_bookings_validation",
            "/Workspace/Shared/TravelAgent_Test/Validation/Hotel_Bookings_Validation_GE"
        )
        
        # TEMPORARILY COMMENTED OUT - Fix the notebook path in Databricks first
        # hotel_validation = create_databricks_task(
        #     "hotel_validation",
        #     "/Workspace/Shared/TravelAgent_Test/Validation/Hotels_Validation_GE"
        # )

    # =====================================================
    # Task Group 3: Bronze to Silver (All tasks run in parallel)
    # =====================================================
    with TaskGroup("bronze_to_silver") as bronze_to_silver_group:
        
        aircrafts_bronze_to_silver = create_databricks_task(
            "aircrafts_bronze_to_silver",
            "/Workspace/Shared/TravelAgent_Test/Bronze_To_Silver/Aircrafts_From_Bronze_To_Silver"
        )
        
        airlines_bronze_to_silver = create_databricks_task(
            "airlines_bronze_to_silver",
            "/Workspace/Shared/TravelAgent_Test/Bronze_To_Silver/Airlines_From_Bronze_To_Silver"
        )
        
        airports_bronze_to_silver = create_databricks_task(
            "airports_bronze_to_silver",
            "/Workspace/Shared/TravelAgent_Test/Bronze_To_Silver/Airports_From_Bronze_To_Silver"
        )
        
        customers_bronze_to_silver = create_databricks_task(
            "customers_bronze_to_silver",
            "/Workspace/Shared/TravelAgent_Test/Bronze_To_Silver/Customers_From_Bronze_To_Silver"
        )
        
        flight_bookings_bronze_to_silver = create_databricks_task(
            "flight_bookings_bronze_to_silver",
            "/Workspace/Shared/TravelAgent_Test/Bronze_To_Silver/Flight_Bookings_From_Bronze_To_Silver"
        )
        
        hotel_bookings_bronze_to_silver = create_databricks_task(
            "hotel_bookings_bronze_to_silver",
            "/Workspace/Shared/TravelAgent_Test/Bronze_To_Silver/Hotel_Bookings_From_Bronze_To_Silver"
        )
        
        hotels_bronze_to_silver = create_databricks_task(
            "hotels_bronze_to_silver",
            "/Workspace/Shared/TravelAgent_Test/Bronze_To_Silver/Hotels_From_Bronze_To_Silver"
        )

    # =====================================================
    # Task Group 4: Silver to Gold
    # Dimensions run in parallel first, then Facts
    # =====================================================
    with TaskGroup("silver_to_gold") as silver_to_gold_group:
        
        # Dimension tables (run in parallel)
        dim_aircraft = create_databricks_task(
            "dim_aircraft",
            "/Workspace/Shared/TravelAgent_Test/Silver_To_Gold/Dim_Aircraft"
        )
        
        dim_airline = create_databricks_task(
            "dim_airline",
            "/Workspace/Shared/TravelAgent_Test/Silver_To_Gold/Dim_Airline"
        )
        
        dim_airport = create_databricks_task(
            "dim_airport",
            "/Workspace/Shared/TravelAgent_Test/Silver_To_Gold/Dim_Airport"
        )
        
        dim_customer = create_databricks_task(
            "dim_customer",
            "/Workspace/Shared/TravelAgent_Test/Silver_To_Gold/Dim_Customer"
        )
        
        dim_date = create_databricks_task(
            "dim_date",
            "/Workspace/Shared/TravelAgent_Test/Silver_To_Gold/Dim_Date"
        )
        
        dim_hotel = create_databricks_task(
            "dim_hotel",
            "/Workspace/Shared/TravelAgent_Test/Silver_To_Gold/Dim_Hotel"
        )
        
        dim_time = create_databricks_task(
            "dim_time",
            "/Workspace/Shared/TravelAgent_Test/Silver_To_Gold/Dim_Time"
        )
        
        # Fact tables (run after dimensions)
        fact_flight_booking = create_databricks_task(
            "fact_flight_booking",
            "/Workspace/Shared/TravelAgent_Test/Silver_To_Gold/Fact_Flight_Booking"
        )
        
        fact_hotel_booking = create_databricks_task(
            "fact_hotel_booking",
            "/Workspace/Shared/TravelAgent_Test/Silver_To_Gold/Fact_Hotel_Booking"
        )
        
        # Dependencies within the Gold layer
        # All dimensions must complete before facts start
        [dim_aircraft, dim_airline, dim_airport, dim_customer, 
         dim_date, dim_time] >> fact_flight_booking
        
        [dim_hotel, dim_customer, dim_date, dim_time] >> fact_hotel_booking

    # =====================================================
    # Define Task Group Dependencies
    # Each group waits for the previous group to complete
    # =====================================================
    s3_to_bronze_group >> validation_group >> bronze_to_silver_group >> silver_to_gold_group
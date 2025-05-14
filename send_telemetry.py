from dotenv import load_dotenv
import json
import time
import random
import os

# import base64
# import hmac
# import hashlib

load_dotenv()

# Import the Azure IoT Central device SDK
from azure.iot.device import Message, exceptions
from azure.iot.device.aio import (
    IoTHubDeviceClient,
    ProvisioningDeviceClient,
)  # Correct import for ProvisioningDeviceClient

# --- Configuration for DPS ---
PROVISIONING_HOST = os.getenv(
    "IOTHUB_DEVICE_DPS_ENDPOINT", "global.azure-devices-provisioning.net"
)
ID_SCOPE = os.getenv("IOTHUB_DEVICE_DPS_ID_SCOPE")
REGISTRATION_ID = os.getenv("IOTHUB_DEVICE_DPS_DEVICE_ID")  # This is your Device ID
SYMMETRIC_KEY = os.getenv(
    "IOTHUB_DEVICE_DPS_DEVICE_KEY"
)  # Primary Key of the group or device

# Telemetry sending interval in seconds
MESSAGE_INTERVAL = 10  # Send data every 10 seconds


def create_telemetry_data():
    """
    Creates a telemetry data payload based on the DTDL model (dtdl.json).
    This function generates random data for demonstration purposes.
    """
    # Based on the DTDL file (dtdl.json)
    # "name" fields are: "radon", "pm2p5", "voc", "co2", "humidity", "temperature", "pressure"
    data = {
        "radon": round(random.uniform(0.1, 4.0), 2),  # Example range for pCi/L
        "pm2p5": round(random.uniform(5.0, 50.0), 2),  # Example range for µg/m³
        "voc": round(random.uniform(50, 1000), 1),  # Example range for ppb
        "co2": round(random.uniform(400, 2000), 0),  # Example range for ppm
        "humidity": round(random.uniform(30.0, 70.0), 1),  # Example range for %
        "temperature": round(random.uniform(18.0, 30.0), 1),  # Example range for °C
        "pressure": round(random.uniform(980.0, 1050.0), 1),  # Example range for hPa
    }
    return data


async def provision_device():
    try:
        """Provisions the device with DPS and returns the IoTHubDeviceClient."""
        provisioning_device_client = ProvisioningDeviceClient.create_from_symmetric_key(
            provisioning_host=PROVISIONING_HOST,
            registration_id=REGISTRATION_ID,
            id_scope=ID_SCOPE,
            symmetric_key=SYMMETRIC_KEY,
        )

        provisioning_device_client.provisioning_payload = {
            "modelId": "dtmi:training101:airthings_4gt;1"
        }

        print(
            f"Provisioning device '{REGISTRATION_ID}' with DPS host '{PROVISIONING_HOST}' and ID scope '{ID_SCOPE}'..."
        )
        try:
            registration_result = await provisioning_device_client.register()
        except Exception as e:
            print(f"Error during DPS registration: {e}")
            raise

        print(f"DPS Registration successful. Status: {registration_result.status}")
        if registration_result.status == "assigned":
            print(
                f"Device has been assigned to IoT Hub: {registration_result.registration_state.assigned_hub}"
            )
            print(f"Device ID: {registration_result.registration_state.device_id}")
            device_client = IoTHubDeviceClient.create_from_symmetric_key(
                symmetric_key=SYMMETRIC_KEY,  # DPS uses the same symmetric key for the device client after provisioning
                hostname=registration_result.registration_state.assigned_hub,
                device_id=registration_result.registration_state.device_id,
            )
            print(device_client)
            return device_client
        else:
            raise RuntimeError(
                f"Device provisioning status was not 'assigned': {registration_result.status}"
            )
    except Exception as e:
        print("Error provisioning device", e)
        return None


async def main():
    print("IoT Central Telemetry Sender for Airthings Device (using DPS)")
    print("Press Ctrl-C to exit")

    if not all([ID_SCOPE, REGISTRATION_ID, SYMMETRIC_KEY]):
        print("Error: One or more DPS environment variables not set:")
        print("- IOTHUB_DEVICE_DPS_ID_SCOPE")
        print("- IOTHUB_DEVICE_DPS_DEVICE_ID")
        print("- IOTHUB_DEVICE_DPS_DEVICE_KEY")
        print(
            "Optional: IOTHUB_DEVICE_DPS_ENDPOINT (defaults to global.azure-devices-provisioning.net)"
        )
        print("Please set these environment variables.")
        return

    # device_client = None
    try:
        # Provision the device and get an IoTHubDeviceClient
        device_client = await provision_device()

        print("Device client created via DPS. Connecting to IoT Hub...")
        # if (device_client):
        #     print('Device client exit', device_client)
        #     print(type(device_client))
        await device_client.connect()
        print("Device connected to IoT Hub.")

        while True:
            telemetry_data = create_telemetry_data()
            msg_txt_formatted = json.dumps(telemetry_data)
            message = Message(msg_txt_formatted)
            message.content_encoding = "utf-8"
            message.content_type = "application/json"

            print(f"Sending message: {msg_txt_formatted}")
            try:
                await device_client.send_message(message)
                # print("Message successfully sent")
            except exceptions.ConnectionDroppedError:
                print("Connection dropped. Attempting to reconnect...")
                try:
                    await device_client.connect()  # Reconnect
                    print("Reconnected. Retrying to send message.")
                    await device_client.send_message(message)
                    print("Message successfully sent after reconnect.")
                except Exception as e_reconnect:
                    print(f"Failed to reconnect or resend: {e_reconnect}")
                    time.sleep(MESSAGE_INTERVAL * 2)
                    continue
            except Exception as e:
                print(f"Error sending message: {e}")

            await asyncio.sleep(
                MESSAGE_INTERVAL
            )  # Use asyncio.sleep for async functions

    except KeyboardInterrupt:
        print("Telemetry sender stopped by user.")
    except RuntimeError as e:
        print(f"Runtime error during provisioning or connection: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if device_client:  # First check if device_client exists
            try:
                if device_client.connected:  # Then check if it's connected
                    print("Shutting down device client...")
                    await device_client.shutdown()
                    print("Device client shut down.")
            except Exception as e:
                print(f"Error during shutdown: {e}")


if __name__ == "__main__":
    # Import asyncio if it's not already imported at the top level
    import asyncio

    try:
        asyncio.run(main())
    except (
        RuntimeError
    ) as e:  # Catch asyncio specific runtime errors like event loop already running
        if "Cannot run the event loop while another loop is running" in str(e):
            print(
                "Asyncio event loop is already running. Please ensure this script is run in an environment where a new loop can be started."
            )
        else:
            raise

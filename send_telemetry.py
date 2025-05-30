from dotenv import load_dotenv
import json
import time
import random
import os
import asyncio

from azure.iot.device import Message, exceptions
from azure.iot.device.aio import (
    IoTHubDeviceClient,
    ProvisioningDeviceClient,
)

load_dotenv()

# --- Configuration for DPS ---
PROVISIONING_HOST = os.getenv(
    "IOTHUB_DEVICE_DPS_ENDPOINT", "global.azure-devices-provisioning.net"
)
ID_SCOPE = os.getenv("IOTHUB_DEVICE_DPS_ID_SCOPE")
REGISTRATION_ID = os.getenv("IOTHUB_DEVICE_DPS_DEVICE_ID")
SYMMETRIC_KEY = os.getenv("IOTHUB_DEVICE_DPS_DEVICE_KEY")

# Send data every x seconds
MESSAGE_INTERVAL = 10
MODEL_ID = "dtmi:training101:airthings_4gt;1"


def create_telemetry_data():
    data = {
        "radon": round(random.uniform(0.1, 4.0), 2),
        "pm2p5": round(random.uniform(5.0, 50.0), 2),
        "voc": round(random.uniform(50, 1000), 1),
        "co2": round(random.uniform(400, 2000), 0),
        "humidity": round(random.uniform(30.0, 70.0), 1),
        "temperature": round(random.uniform(18.0, 30.0), 1),
        "pressure": round(random.uniform(980.0, 1050.0), 1),
    }
    return data


async def provision_device():
    """Provisions the device with DPS and returns the IoTHubDeviceClient."""
    provisioning_device_client = ProvisioningDeviceClient.create_from_symmetric_key(
        provisioning_host=PROVISIONING_HOST,
        registration_id=REGISTRATION_ID,
        id_scope=ID_SCOPE,
        symmetric_key=SYMMETRIC_KEY,
    )

    provisioning_device_client.provisioning_payload = {"modelId": MODEL_ID}

    print(
        f"Provisioning device '{REGISTRATION_ID}' with DPS host '{PROVISIONING_HOST}' and ID scope '{ID_SCOPE}'..."
    )

    registration_result = await provisioning_device_client.register()

    print(f"DPS Registration successful. Status: {registration_result.status}")
    if registration_result.status == "assigned":
        print(
            f"Device has been assigned to IoT Hub: {registration_result.registration_state.assigned_hub}"
        )
        print(f"Device ID: {registration_result.registration_state.device_id}")
        device_client = IoTHubDeviceClient.create_from_symmetric_key(
            symmetric_key=SYMMETRIC_KEY,
            hostname=registration_result.registration_state.assigned_hub,
            device_id=registration_result.registration_state.device_id,
        )
        print(device_client)
        return device_client
    else:
        raise RuntimeError(
            f"Device provisioning status was not 'assigned': {registration_result.status}"
        )


async def execute_property_listener(device_client):
    """Listen for desired property changes and respond with reported property updates."""
    while True:
        try:
            # Wait for desired property updates
            patch = (
                await device_client.receive_twin_desired_properties_patch()
            )  # blocking call
            print(f"Received property patch: {patch}")

            # Create the reported property response
            reported_properties = {}

            # Check if GeopointProperty is in the patch
            if "GeopointProperty" in patch:
                # Extract the desired value
                geopoint_value = patch["GeopointProperty"]

                # Create a reported property with acknowledgment
                reported_properties["GeopointProperty"] = {
                    "value": geopoint_value,
                    "ac": 200,  # Status code for success
                    "av": patch.get("$version", 0),  # Version from the desired property
                    "ad": "Property accepted",  # Description
                }

                print(f"Acknowledging GeopointProperty update: {geopoint_value}")

            # Send the reported properties update if we have anything to report
            if reported_properties:
                await device_client.patch_twin_reported_properties(reported_properties)
                print(f"Reported properties updated: {reported_properties}")

        except Exception as e:
            print(f"Error in property listener: {e}")
            # Add a small delay before retrying to avoid tight loops on persistent errors
            await asyncio.sleep(1)


async def main():
    print("IoT Central Telemetry Sender for AirHub Device (using DPS)")
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

    try:
        device_client = await provision_device()

        print("Device client created via DPS. Connecting to IoT Hub...")
        await device_client.connect()
        print("Device connected to IoT Hub.")

        listeners = asyncio.gather(
            execute_property_listener(device_client),
        )

        while True:
            telemetry_data = create_telemetry_data()
            msg_txt_formatted = json.dumps(telemetry_data)
            message = Message(msg_txt_formatted)
            message.content_encoding = "utf-8"
            message.content_type = "application/json"

            print(f"Sending message: {msg_txt_formatted}")
            await device_client.send_message(message)

            await asyncio.sleep(MESSAGE_INTERVAL)

    except KeyboardInterrupt:
        print("Telemetry sender stopped by user.")
    except RuntimeError as e:
        print(f"Runtime error during provisioning or connection: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if not listeners.done():
            listeners.set_result("DONE")

        listeners.cancel()

        if device_client:
            try:
                if device_client.connected:
                    print("Shutting down device client...")
                    await device_client.shutdown()
                    print("Device client shut down.")
            except Exception as e:
                print(f"Error during shutdown: {e}")


if __name__ == "__main__":
    asyncio.run(main())

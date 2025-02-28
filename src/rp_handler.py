import runpod
from runpod.serverless.utils import rp_upload
import json
import urllib.request
import urllib.parse
import time
import os
import requests
import base64
from io import BytesIO

# Time to wait between API check attempts in milliseconds
COMFY_API_AVAILABLE_INTERVAL_MS = 50
# Maximum number of API check attempts
COMFY_API_AVAILABLE_MAX_RETRIES = 500
# Time to wait between poll attempts in milliseconds
COMFY_POLLING_INTERVAL_MS = int(os.environ.get("COMFY_POLLING_INTERVAL_MS", 250))
# Maximum number of poll attempts
COMFY_POLLING_MAX_RETRIES = int(os.environ.get("COMFY_POLLING_MAX_RETRIES", 500))
# Host where ComfyUI is running
COMFY_HOST = "127.0.0.1:8188"
# Enforce a clean state after each job is done
# see https://docs.runpod.io/docs/handler-additional-controls#refresh-worker
REFRESH_WORKER = os.environ.get("REFRESH_WORKER", "false").lower() == "true"


def validate_input(job_input):
    """
    Validates the input for the handler function.
    """
    # Validate if job_input is provided
    if job_input is None:
        return None, "Please provide input"

    # Check if input is a string and try to parse it as JSON
    if isinstance(job_input, str):
        try:
            job_input = json.loads(job_input)
        except json.JSONDecodeError:
            return None, "Invalid JSON format in input"

    # Validate 'workflow' in input
    workflow = job_input.get("workflow")
    if workflow is None:
        return None, "Missing 'workflow' parameter"

    # Validate 'images' in input, if provided
    images = job_input.get("images")
    if images is not None:
        if not isinstance(images, list) or not all(
            "name" in image and "image" in image for image in images
        ):
            return (
                None,
                "'images' must be a list of objects with 'name' and 'image' keys",
            )
    
    # Validate user_id (optional)
    user_id = job_input.get("user_id", "default-user")
    if not isinstance(user_id, str):
        return None, "'user_id' must be a string"

    # Return validated data and no error
    return {"workflow": workflow, "images": images, "user_id": user_id}, None

def check_server(url, retries=500, delay=50):
    """
    Check if a server is reachable via HTTP GET request

    Args:
    - url (str): The URL to check
    - retries (int, optional): The number of times to attempt connecting to the server. Default is 50
    - delay (int, optional): The time in milliseconds to wait between retries. Default is 500

    Returns:
    bool: True if the server is reachable within the given number of retries, otherwise False
    """

    for i in range(retries):
        try:
            response = requests.get(url)

            # If the response status code is 200, the server is up and running
            if response.status_code == 200:
                print(f"runpod-worker-comfy - API is reachable")
                return True
        except requests.RequestException as e:
            # If an exception occurs, the server may not be ready
            pass

        # Wait for the specified delay before retrying
        time.sleep(delay / 1000)

    print(
        f"runpod-worker-comfy - Failed to connect to server at {url} after {retries} attempts."
    )
    return False


def upload_images(images):
    """
    Upload a list of base64 encoded images to the ComfyUI server using the /upload/image endpoint.

    Args:
        images (list): A list of dictionaries, each containing the 'name' of the image and the 'image' as a base64 encoded string.
        server_address (str): The address of the ComfyUI server.

    Returns:
        list: A list of responses from the server for each image upload.
    """
    if not images:
        return {"status": "success", "message": "No images to upload", "details": []}

    responses = []
    upload_errors = []

    print(f"runpod-worker-comfy - image(s) upload")

    for image in images:
        name = image["name"]
        image_data = image["image"]
        blob = base64.b64decode(image_data)

        # Prepare the form data
        files = {
            "image": (name, BytesIO(blob), "image/png"),
            "overwrite": (None, "true"),
        }

        # POST request to upload the image
        response = requests.post(f"http://{COMFY_HOST}/upload/image", files=files)
        if response.status_code != 200:
            upload_errors.append(f"Error uploading {name}: {response.text}")
        else:
            responses.append(f"Successfully uploaded {name}")

    if upload_errors:
        print(f"runpod-worker-comfy - image(s) upload with errors")
        return {
            "status": "error",
            "message": "Some images failed to upload",
            "details": upload_errors,
        }

    print(f"runpod-worker-comfy - image(s) upload complete")
    return {
        "status": "success",
        "message": "All images uploaded successfully",
        "details": responses,
    }


def queue_workflow(workflow):
    """
    Queue a workflow to be processed by ComfyUI

    Args:
        workflow (dict): A dictionary containing the workflow to be processed

    Returns:
        dict: The JSON response from ComfyUI after processing the workflow
    """

    # The top level element "prompt" is required by ComfyUI
    data = json.dumps({"prompt": workflow}).encode("utf-8")

    req = urllib.request.Request(f"http://{COMFY_HOST}/prompt", data=data)
    return json.loads(urllib.request.urlopen(req).read())


def get_history(prompt_id):
    """
    Retrieve the history of a given prompt using its ID

    Args:
        prompt_id (str): The ID of the prompt whose history is to be retrieved

    Returns:
        dict: The history of the prompt, containing all the processing steps and results
    """
    with urllib.request.urlopen(f"http://{COMFY_HOST}/history/{prompt_id}") as response:
        return json.loads(response.read())


def base64_encode(img_path):
    """
    Returns base64 encoded image.

    Args:
        img_path (str): The path to the image

    Returns:
        str: The base64 encoded image
    """
    with open(img_path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode("utf-8")
        return f"{encoded_string}"


def process_output_images(outputs, job_id, user_id="anonymous"):
    """
    Process output images from ComfyUI and either upload to R2 or encode as base64
    """
    # The path where ComfyUI stores the generated images
    COMFY_OUTPUT_PATH = os.environ.get("COMFY_OUTPUT_PATH", "/comfyui/output")

    output_images = {}

    for node_id, node_output in outputs.items():
        if "images" in node_output:
            for image in node_output["images"]:
                output_images = os.path.join(image["subfolder"], image["filename"])

    print(f"runpod-worker-comfy - image generation is done")

    # expected image output folder
    local_image_path = f"{COMFY_OUTPUT_PATH}/{output_images}"

    print(f"runpod-worker-comfy - {local_image_path}")

    # The image is in the output folder
    if os.path.exists(local_image_path):
        if os.environ.get("R2_ENDPOINT", False):
            # Upload to R2
            try:
                # Generate a unique path for the image
                image_id = str(uuid.uuid4())
                image_path = f"{user_id}/{image_id}.png"
                
                # Get S3 client for R2
                s3 = boto3.client(
                    's3',
                    endpoint_url=os.environ.get('R2_ENDPOINT'),
                    aws_access_key_id=os.environ.get('R2_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.environ.get('R2_SECRET_ACCESS_KEY')
                )
                
                # Upload to R2
                with open(local_image_path, 'rb') as file_data:
                    s3.upload_fileobj(
                        file_data,
                        os.environ.get('IMAGES_BUCKET'),
                        image_path,
                        ExtraArgs={'ContentType': 'image/png'}
                    )
                
                # Build the full R2 URL (optional)
                r2_endpoint = os.environ.get('R2_ENDPOINT')
                bucket_name = os.environ.get('IMAGES_BUCKET')
                if r2_endpoint.endswith('/'):
                    r2_endpoint = r2_endpoint[:-1]
                    
                image_url = f"{r2_endpoint}/{bucket_name}/{image_path}"
                
                print(f"runpod-worker-comfy - image uploaded to R2 for user {user_id}")
                
                # Return the R2 URL and path
                return {
                    "status": "success",
                    "message": image_url,
                    "image_path": image_path  # Include the path for database storage
                }
            except Exception as e:
                print(f"runpod-worker-comfy - R2 upload failed: {str(e)}")
                # Fall back to base64 if R2 upload fails
                image = base64_encode(local_image_path)
                return {
                    "status": "error",
                    "message": f"R2 upload failed: {str(e)}. Falling back to base64 image.",
                    "image": image
                }
        else:
            # base64 image
            image = base64_encode(local_image_path)
            print("runpod-worker-comfy - the image was generated and converted to base64")
            return {
                "status": "success",
                "message": image,
            }
    else:
        print("runpod-worker-comfy - the image does not exist in the output folder")
        return {
            "status": "error",
            "message": f"the image does not exist in the specified output folder: {local_image_path}",
        }

def handler(job):
    """
    The main function that handles a job of generating an image.
    """
    job_input = job["input"]
    
    # Make sure that the input is valid
    validated_data, error_message = validate_input(job_input)
    if error_message:
        return {"error": error_message}
    
    # Extract validated data
    workflow = validated_data["workflow"]
    images = validated_data.get("images")
    
    # Extract user_id from input, default to "default-user" if not provided
    user_id = job_input.get("user_id", "default-user")
    
    # Make sure that the ComfyUI API is available
    check_server(
        f"http://{COMFY_HOST}",
        COMFY_API_AVAILABLE_MAX_RETRIES,
        COMFY_API_AVAILABLE_INTERVAL_MS,
    )

    # Upload images if they exist
    upload_result = upload_images(images)

    if upload_result["status"] == "error":
        return upload_result

    # Queue the workflow
    try:
        queued_workflow = queue_workflow(workflow)
        prompt_id = queued_workflow["prompt_id"]
        print(f"runpod-worker-comfy - queued workflow with ID {prompt_id}")
    except Exception as e:
        return {"error": f"Error queuing workflow: {str(e)}"}

    # Poll for completion
    print(f"runpod-worker-comfy - wait until image generation is complete")
    retries = 0
    try:
        while retries < COMFY_POLLING_MAX_RETRIES:
            history = get_history(prompt_id)

            # Exit the loop if we have found the history
            if prompt_id in history and history[prompt_id].get("outputs"):
                break
            else:
                # Wait before trying again
                time.sleep(COMFY_POLLING_INTERVAL_MS / 1000)
                retries += 1
        else:
            return {"error": "Max retries reached while waiting for image generation"}
    except Exception as e:
        return {"error": f"Error waiting for image generation: {str(e)}"}

    # Get the generated image and return it as URL in R2 or as base64
    # Pass the user_id to process_output_images
    images_result = process_output_images(history[prompt_id].get("outputs"), job["id"], user_id)

    result = {**images_result, "refresh_worker": REFRESH_WORKER}

    return result

# Start the handler only if this script is run directly
if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})

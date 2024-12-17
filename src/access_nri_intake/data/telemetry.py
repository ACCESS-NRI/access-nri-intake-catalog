import ast
import asyncio
import warnings

import httpx
from IPython import get_ipython

TELEMETRY_SERVER_URL = "https://intake-telemetry-bb870061f91a.herokuapp.com"

TELEMETRY_REGISTRED_FUNCTIONS = [
    "esm_datastore.search",
    "DfFileCatalog.search",
]


def send_api_request(function_name, kwargs):
    telemetry_data = {
        "name": f"CT_testing_{function_name}_ipy_extensions",
        "search": kwargs,
    }

    endpoint = f"{TELEMETRY_SERVER_URL}/telemetry/update"

    async def send_telemetry(data):
        headers = {"Content-Type": "application/json"}
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(endpoint, json=data, headers=headers)
                response.raise_for_status()

                print(f"Telemetry data sent: {response.json()}")
            except httpx.RequestError as e:
                warnings.warn(
                    f"Request failed: {e}", category=RuntimeWarning, stacklevel=2
                )

    # Check if there's an existing event loop, otherwise create a new one
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        loop.create_task(send_telemetry(telemetry_data))
    else:
        loop.run_until_complete(send_telemetry(telemetry_data))
    return None


def capture_datastore_searches(info):
    """
    Use the AST module to parse the code that we are executing & send an API call
    if we

    """
    code = info.raw_cell

    # Remove lines that contain IPython magic commands
    code = "\n".join(
        line for line in code.splitlines() if not line.strip().startswith("%")
    )

    tree = ast.parse(code)
    user_namespace = get_ipython().user_ns

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                # Check if the attribute is a method call on a class instance or a module function
                if isinstance(node.func.value, ast.Name):
                    instance_name = node.func.value.id
                    method_name = node.func.attr
                    try:
                        # Evaluate the instance to get its class name
                        instance = eval(instance_name, globals(), user_namespace)
                        class_name = instance.__class__.__name__
                        func_name = f"{class_name}.{method_name}"
                    except Exception as e:
                        print(f"Error evaluating instance: {e}")
                        continue

            if func_name in TELEMETRY_REGISTRED_FUNCTIONS:
                # args = [ast.dump(arg) for arg in node.args]
                kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in node.keywords}
                send_api_request(
                    func_name,
                    # args,
                    kwargs,
                )

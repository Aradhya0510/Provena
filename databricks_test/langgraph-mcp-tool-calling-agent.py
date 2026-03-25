# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: Author and deploy an MCP tool-calling LangGraph agent
# MAGIC
# MAGIC This notebook shows how to author a LangGraph agent that connects to MCP servers hosted on Databricks. LangGraph's graph-based architecture gives you complete control over agent behavior, making it the right choice when you need custom workflows or multi-step reasoning patterns.
# MAGIC
# MAGIC Connect your agent to data and tools through MCP servers. Databricks provides managed MCP servers for Unity Catalog functions, vector search, and Genie spaces. You can also connect to custom MCP servers that you host as Databricks Apps. See [MCP on Databricks](https://docs.databricks.com/aws/en/generative-ai/mcp/).
# MAGIC
# MAGIC In this notebook, you:
# MAGIC
# MAGIC - Author a LangGraph agent
# MAGIC - Connect the agent to MCP servers to access Databricks-hosted tools
# MAGIC - Test the agent and evaluate its responses using MLflow Evaluation
# MAGIC - Log the agent with MLflow and deploy it to a model serving endpoint
# MAGIC
# MAGIC This notebook uses the  [`ResponsesAgent`](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ResponsesAgent) for Databrick compatibility.
# MAGIC
# MAGIC To learn more about authoring an agent using Mosaic AI Agent Framework, see Databricks documentation ([AWS](https://docs.databricks.com/aws/generative-ai/agent-framework/author-agent) | [Azure](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/create-chat-model)).
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Address all `TODO`s in this notebook.

# COMMAND ----------

# MAGIC %pip install -U -qqqq --force-reinstall databricks-langchain databricks-agents uv

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the agent code
# MAGIC
# MAGIC Define the agent code in a single cell below. This lets you easily write the agent
# MAGIC code to a local Python file, using the `%%writefile` magic command, for subsequent
# MAGIC logging and deployment.
# MAGIC
# MAGIC **What this code does at a high level:**
# MAGIC
# MAGIC 1. **Connect to MCP servers using adapters**
# MAGIC     The `DatabricksMCPServer` and `DatabricksMultiServerMCPClient` from `databricks_langchain` handle:
# MAGIC     - Connections to Databricks MCP servers
# MAGIC     - Authentication
# MAGIC     - Automatic tool discovery and conversion to LangChain-compatible format
# MAGIC
# MAGIC 2. **Build a LangGraph agent workflow using LangGraph `StateGraph`**
# MAGIC
# MAGIC 3. **Handle streaming responses**
# MAGIC     The `MCPToolCallingAgent` class wraps the LangGraph workflow to:
# MAGIC     - Process streaming events from the agent graph in real-time
# MAGIC     - Convert LangChain message formats to Mosaic AI-compatible format
# MAGIC     - Enable MLflow tracing for each step of the agent workflow
# MAGIC
# MAGIC 4. **Wrap with ResponsesAgent**
# MAGIC     The agent is wrapped using `ResponsesAgent` for compatibility with Databricks
# MAGIC     features like evaluation, deployment, and feedback collection.
# MAGIC
# MAGIC 5. **MLflow autotracing**
# MAGIC     Enable MLflow autologging to automatically trace LLM calls, tool invocations,
# MAGIC     and agent state transitions.
# MAGIC
# MAGIC #### Agent tools
# MAGIC
# MAGIC This example connects to the Unity Catalog functions MCP server to access
# MAGIC `system.ai.python_exec` (a built-in Python code interpreter). The code also
# MAGIC includes commented-out examples for connecting to:
# MAGIC - Custom MCP servers (hosted as Databricks Apps)
# MAGIC - Vector search MCP servers (for semantic search over your data)
# MAGIC

# COMMAND ----------

# MAGIC %%writefile agent.py
# MAGIC
# MAGIC import asyncio
# MAGIC from typing import Annotated, Any, AsyncGenerator, Generator, Optional, Sequence, TypedDict, Union
# MAGIC
# MAGIC import mlflow
# MAGIC import nest_asyncio
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks_langchain import (
# MAGIC     ChatDatabricks,
# MAGIC     DatabricksMCPServer,
# MAGIC     DatabricksMultiServerMCPClient,
# MAGIC )
# MAGIC from langchain.messages import AIMessage, AIMessageChunk, AnyMessage
# MAGIC from langchain_core.language_models import LanguageModelLike
# MAGIC from langchain_core.runnables import RunnableConfig, RunnableLambda
# MAGIC from langchain_core.tools import BaseTool
# MAGIC from langgraph.graph import END, StateGraph
# MAGIC from langgraph.graph.message import add_messages
# MAGIC from langgraph.prebuilt.tool_node import ToolNode
# MAGIC from mlflow.pyfunc import ResponsesAgent
# MAGIC from mlflow.types.responses import (
# MAGIC     ResponsesAgentRequest,
# MAGIC     ResponsesAgentResponse,
# MAGIC     ResponsesAgentStreamEvent,
# MAGIC     output_to_responses_items_stream,
# MAGIC     to_chat_completions_input,
# MAGIC )
# MAGIC from langchain_core.messages.tool import ToolMessage
# MAGIC import json
# MAGIC
# MAGIC nest_asyncio.apply()
# MAGIC ############################################
# MAGIC ## Define your LLM endpoint and system prompt
# MAGIC ############################################
# MAGIC LLM_ENDPOINT_NAME = "databricks-claude-3-7-sonnet"
# MAGIC llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
# MAGIC
# MAGIC # TODO: Update with your system prompt
# MAGIC system_prompt = """
# MAGIC You are a helpful assistant that can run Python code.
# MAGIC """
# MAGIC
# MAGIC # TODO: Choose your MCP server connection type and setup the Workspace Clients for Authentication
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # Managed MCP Server — simplest setup
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # Databricks manages this connection automatically using your workspace settings
# MAGIC # and Personal Access Token (PAT) authentication.
# MAGIC
# MAGIC workspace_client = WorkspaceClient()
# MAGIC
# MAGIC host = workspace_client.config.host
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # Custom MCP Server — hosted as a Databricks App
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # Use this if you’re running your own MCP server in Databricks.
# MAGIC # These require OAuth with a service principal for machine-to-machine (M2M) auth.
# MAGIC #
# MAGIC # Follow the insturctions here in order to create a SP, grant the SP query permissions on your app and then mint a client id and # secret. https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m
# MAGIC #
# MAGIC # Uncomment and fill in the settings below to use a custom MCP server.
# MAGIC #
# MAGIC # import os
# MAGIC # custom_mcp_server_workspace_client = WorkspaceClient(
# MAGIC #     host="<DATABRICKS_WORKSPACE_URL>",
# MAGIC #     client_id=os.getenv("DATABRICKS_CLIENT_ID"),
# MAGIC #     client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
# MAGIC #     auth_type="oauth-m2m",  # Enables service principal authentication
# MAGIC # )
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # OBO Setup
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # In order to use OBO, uncomment the code below and pass this workspace client to the appropriate McpServer below
# MAGIC #
# MAGIC # from databricks_ai_bridge import ModelServingUserCredentials
# MAGIC # obo_workspace_client = WorkspaceClient(credentials_strategy=ModelServingUserCredentials())
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Configure MCP Servers for your agent
# MAGIC ##
# MAGIC ## This section sets up server connections so your agent can retrieve data or take actions.
# MAGIC
# MAGIC ## There are three connection types:
# MAGIC ## 1. Managed MCP servers — fully managed by Databricks
# MAGIC ## 2. External MCP servers — hosted outside Databricks but proxied through a
# MAGIC ##    Managed MCP server proxy
# MAGIC ## 3. Custom MCP servers — MCP servers hosted as Databricks Apps
# MAGIC ##
# MAGIC ###############################################################################
# MAGIC databricks_mcp_client = DatabricksMultiServerMCPClient(
# MAGIC     [
# MAGIC         DatabricksMCPServer(
# MAGIC             name="system-ai",
# MAGIC             url=f"{host}/api/2.0/mcp/functions/system/ai",
# MAGIC         ),
# MAGIC         # DatabricksMCPServer(
# MAGIC         #     name="custom_mcp",
# MAGIC         #     url="custom_app_url",
# MAGIC         #     workspace_client=custom_mcp_server_workspace_client
# MAGIC         # ),
# MAGIC         # DatabricksMCPServer(
# MAGIC         #     name="obo_vs_client",
# MAGIC         #     url=f"{host}/api/2.0/mcp/vector-search/system/ai",
# MAGIC         #     workspace_client=obo_workspace_client
# MAGIC         # )
# MAGIC     ]
# MAGIC )
# MAGIC
# MAGIC
# MAGIC # The state for the agent workflow, including the conversation and any custom data
# MAGIC class AgentState(TypedDict):
# MAGIC     messages: Annotated[Sequence[AnyMessage], add_messages]
# MAGIC     custom_inputs: Optional[dict[str, Any]]
# MAGIC     custom_outputs: Optional[dict[str, Any]]
# MAGIC
# MAGIC
# MAGIC def create_tool_calling_agent(
# MAGIC     model: LanguageModelLike,
# MAGIC     tools: Union[ToolNode, Sequence[BaseTool]],
# MAGIC     system_prompt: Optional[str] = None,
# MAGIC ):
# MAGIC     model = model.bind_tools(tools)  # Bind tools to the model
# MAGIC
# MAGIC     # Function to check if agent should continue or finish based on last message
# MAGIC     def should_continue(state: AgentState):
# MAGIC         messages = state["messages"]
# MAGIC         last_message = messages[-1]
# MAGIC         # If function (tool) calls are present, continue; otherwise, end
# MAGIC         if isinstance(last_message, AIMessage) and last_message.tool_calls:
# MAGIC             return "continue"
# MAGIC         else:
# MAGIC             return "end"
# MAGIC
# MAGIC     # Preprocess: optionally prepend a system prompt to the conversation history
# MAGIC     if system_prompt:
# MAGIC         preprocessor = RunnableLambda(
# MAGIC             lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
# MAGIC         )
# MAGIC     else:
# MAGIC         preprocessor = RunnableLambda(lambda state: state["messages"])
# MAGIC
# MAGIC     model_runnable = preprocessor | model  # Chain the preprocessor and the model
# MAGIC
# MAGIC     # The function to invoke the model within the workflow
# MAGIC     def call_model(
# MAGIC         state: AgentState,
# MAGIC         config: RunnableConfig,
# MAGIC     ):
# MAGIC         response = model_runnable.invoke(state, config)
# MAGIC         return {"messages": [response]}
# MAGIC
# MAGIC     workflow = StateGraph(AgentState)  # Create the agent's state machine
# MAGIC
# MAGIC     workflow.add_node("agent", RunnableLambda(call_model))  # Agent node (LLM)
# MAGIC     workflow.add_node("tools", ToolNode(tools))  # Tools node
# MAGIC
# MAGIC     workflow.set_entry_point("agent")  # Start at agent node
# MAGIC     workflow.add_conditional_edges(
# MAGIC         "agent",
# MAGIC         should_continue,
# MAGIC         {
# MAGIC             "continue": "tools",  # If the model requests a tool call, move to tools node
# MAGIC             "end": END,  # Otherwise, end the workflow
# MAGIC         },
# MAGIC     )
# MAGIC     workflow.add_edge("tools", "agent")  # After tools are called, return to agent node
# MAGIC
# MAGIC     # Compile and return the tool-calling agent workflow
# MAGIC     return workflow.compile()
# MAGIC
# MAGIC
# MAGIC # ResponsesAgent class to wrap the compiled agent and make it compatible with Mosaic AI Responses API
# MAGIC class LangGraphResponsesAgent(ResponsesAgent):
# MAGIC     def __init__(self, agent):
# MAGIC         self.agent = agent
# MAGIC
# MAGIC     # Make a prediction (single-step) for the agent
# MAGIC     def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
# MAGIC         outputs = [
# MAGIC             event.item
# MAGIC             for event in self.predict_stream(request)
# MAGIC             if event.type == "response.output_item.done" or event.type == "error"
# MAGIC         ]
# MAGIC         return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)
# MAGIC
# MAGIC     async def _predict_stream_async(
# MAGIC         self,
# MAGIC         request: ResponsesAgentRequest,
# MAGIC     ) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
# MAGIC         cc_msgs = to_chat_completions_input([i.model_dump() for i in request.input])
# MAGIC         # Stream events from the agent graph
# MAGIC         async for event in self.agent.astream(
# MAGIC             {"messages": cc_msgs}, stream_mode=["updates", "messages"]
# MAGIC         ):
# MAGIC             if event[0] == "updates":
# MAGIC                 # Stream updated messages from the workflow nodes
# MAGIC                 for node_data in event[1].values():
# MAGIC                     if len(node_data.get("messages", [])) > 0:
# MAGIC                         all_messages = []
# MAGIC                         for msg in node_data["messages"]:
# MAGIC                             if isinstance(msg, ToolMessage) and not isinstance(msg.content, str):
# MAGIC                                 msg.content = json.dumps(msg.content)
# MAGIC                             all_messages.append(msg)
# MAGIC                         for item in output_to_responses_items_stream(all_messages):
# MAGIC                             yield item
# MAGIC             elif event[0] == "messages":
# MAGIC                 # Stream generated text message chunks
# MAGIC                 try:
# MAGIC                     chunk = event[1][0]
# MAGIC                     if isinstance(chunk, AIMessageChunk) and (content := chunk.content):
# MAGIC                         yield ResponsesAgentStreamEvent(
# MAGIC                             **self.create_text_delta(delta=content, item_id=chunk.id),
# MAGIC                         )
# MAGIC                 except:
# MAGIC                     pass
# MAGIC
# MAGIC     # Stream predictions for the agent, yielding output as it's generated
# MAGIC     def predict_stream(
# MAGIC         self, request: ResponsesAgentRequest
# MAGIC     ) -> Generator[ResponsesAgentStreamEvent, None, None]:
# MAGIC         agen = self._predict_stream_async(request)
# MAGIC
# MAGIC         try:
# MAGIC             loop = asyncio.get_event_loop()
# MAGIC         except RuntimeError:
# MAGIC             loop = asyncio.new_event_loop()
# MAGIC             asyncio.set_event_loop(loop)
# MAGIC
# MAGIC         ait = agen.__aiter__()
# MAGIC
# MAGIC         while True:
# MAGIC             try:
# MAGIC                 item = loop.run_until_complete(ait.__anext__())
# MAGIC             except StopAsyncIteration:
# MAGIC                 break
# MAGIC             else:
# MAGIC                 yield item
# MAGIC
# MAGIC
# MAGIC # Initialize the entire agent, including MCP tools and workflow
# MAGIC def initialize_agent():
# MAGIC     """Initialize the agent with MCP tools"""
# MAGIC     # Create MCP tools from the configured servers
# MAGIC     mcp_tools = asyncio.run(databricks_mcp_client.get_tools())
# MAGIC
# MAGIC     # Create the agent graph with an LLM, tool set, and system prompt (if given)
# MAGIC     agent = create_tool_calling_agent(llm, mcp_tools, system_prompt)
# MAGIC     return LangGraphResponsesAgent(agent)
# MAGIC
# MAGIC
# MAGIC mlflow.langchain.autolog()
# MAGIC AGENT = initialize_agent()
# MAGIC mlflow.models.set_model(AGENT)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output and tool-calling abilities. Since this notebook called `mlflow.langchain.autolog()`, you can view the trace for each step the agent takes.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# ==============================================================================
# TODO: ONLY UNCOMMENT AND EDIT THIS SECTION IF YOU ARE USING OAUTH/SERVICE PRINCIPAL FOR CUSTOM MCP SERVERS.
#       For managed MCP (the default), LEAVE THIS SECTION COMMENTED OUT.
# ==============================================================================

# import os

# # Set your Databricks client ID and client secret for service principal authentication.
# DATABRICKS_CLIENT_ID = "<YOUR_CLIENT_ID>"
# client_secret_scope_name = "<YOUR_SECRET_SCOPE>"
# client_secret_key_name = "<YOUR_SECRET_KEY_NAME>"

# # Load your service principal credentials into environment variables
# os.environ["DATABRICKS_CLIENT_ID"] = DATABRICKS_CLIENT_ID
# os.environ["DATABRICKS_CLIENT_SECRET"] = dbutils.secrets.get(scope=client_secret_scope_name, key=client_secret_key_name)


# COMMAND ----------

from agent import AGENT

AGENT.predict({"input": [{"role": "user", "content": "What is 7*6 in Python?"}]})

# COMMAND ----------

for chunk in AGENT.predict_stream(
    {"input": [{"role": "user", "content": "What is 7*6 in Python?"}]}
):
    print(chunk, "-----------\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log the agent as an MLflow model
# MAGIC
# MAGIC Log the agent as code from the `agent.py` file. See [Deploy an agent that connects to Databricks MCP servers](https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp#deploy-your-agent).

# COMMAND ----------

import mlflow
from agent import LLM_ENDPOINT_NAME
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksFunction
from pkg_resources import get_distribution

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME), 
    DatabricksFunction(function_name="system.ai.python_exec")
]

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model="agent.py",
        resources=resources,
        pip_requirements=[
            f"langgraph=={get_distribution('langgraph').version}",
            f"mcp=={get_distribution('mcp').version}",
            f"databricks-mcp=={get_distribution('databricks-mcp').version}",
            f"databricks-langchain=={get_distribution('databricks-langchain').version}",
        ]
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://docs.databricks.com/mlflow3/genai/eval-monitor)
# MAGIC
# MAGIC You can edit the requests or expected responses in your evaluation dataset and run evaluation as you iterate your agent, leveraging mlflow to track the computed quality metrics.
# MAGIC
# MAGIC Evaluate your agent with one of our [predefined LLM scorers](https://docs.databricks.com/mlflow3/genai/eval-monitor/predefined-judge-scorers), or try adding [custom metrics](https://docs.databricks.com/mlflow3/genai/eval-monitor/custom-scorers).

# COMMAND ----------

import mlflow
from mlflow.genai.scorers import RelevanceToQuery, Safety, RetrievalRelevance, RetrievalGroundedness

eval_dataset = [
    {
        "inputs": {
            "input": [
                {
                    "role": "user",
                    "content": "Calculate the 15th Fibonacci number"
                }
            ]
        },
        "expected_response": "The 15th Fibonacci number is 610."
    }
]

eval_results = mlflow.genai.evaluate(
    data=eval_dataset,
    predict_fn=lambda input: AGENT.predict({"input": input}),
    scorers=[RelevanceToQuery(), Safety()], # add more scorers here if they're applicable
)

# Review the evaluation results in the MLfLow UI (see console output)

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_agent_info.run_id}/agent",
    input_data={"input": [{"role": "user", "content": "What is 7*6 in Python?"}]},
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Before you deploy the agent, you must register the agent to Unity Catalog.
# MAGIC
# MAGIC - **TODO** Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# TODO: define the catalog, schema, and model name for your UC model
catalog = ""
schema = ""
model_name = ""
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents

agents.deploy(
    UC_MODEL_NAME, 
    uc_registered_model_info.version,
    # ==============================================================================
    # TODO: ONLY UNCOMMENT AND CONFIGURE THE ENVIRONMENT_VARS SECTION BELOW
    #       IF YOU ARE USING OAUTH/SERVICE PRINCIPAL FOR CUSTOM MCP SERVERS.
    #       For managed MCP (the default), LEAVE THIS SECTION COMMENTED OUT.
    # ==============================================================================
    # environment_vars={
    #     "DATABRICKS_CLIENT_ID": DATABRICKS_CLIENT_ID,
    #     "DATABRICKS_CLIENT_SECRET": f"{{{{secrets/{client_secret_scope_name}/{client_secret_key_name}}}}}"
    # },
    tags = {"endpointSource": "docs"},
    deploy_feedback_model=False
)

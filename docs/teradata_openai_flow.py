from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.client import Client
from diagrams.onprem.compute import Server
from diagrams.programming.language import Python
from diagrams.programming.framework import FastAPI

with Diagram("Teradata OpenAI Client SQL Function Flow", show=False, direction="LR"):

    with Cluster("Client"):
        client = Python("Python/SQL Client")

    with Cluster("Teradata Vantage", graph_attr={"bgcolor": "orange", "style": "filled"}):
        parsing_engine = Server("Parsing Engine")

        with Cluster("AMPs (Parallel Processing)"):
            amp1 = Server("AMP 1")
            amp2 = Server("AMP 2")
            amp3 = Server("AMP 3")
            amp_more = Server("...")
            amp4 = Server("AMP N")

            amps = [amp4, amp_more, amp3, amp2, amp1]

    with Cluster("LLM Service", graph_attr={"style": "filled"}):
        llm_api = FastAPI("LLM Service\n(OpenAI API)")

    # Flow: Client submits SQL to Parsing Engine
    client >> Edge(label="SELECT * FROM CompleteChat") >> parsing_engine

    # Parsing Engine distributes work to AMPs
    parsing_engine >> Edge(label="Distribute Work") >> amps

    # Double-headed arrow between AMPs and LLM
    for amp in amps:
        amp << Edge(label="API Calls / Responses", dir="both") >> llm_api
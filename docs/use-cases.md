# MCPC Use Cases

## Why MCPC Exists

MCPC was created to solve a critical limitation in LLM tool interactions: **maintaining conversational flow while running background tasks**.

The standard MCP protocol follows a synchronous request-response pattern, which blocks the conversation until a tool completes. This creates poor UX when:

1. You want to chat with an LLM while a long-running task executes
2. You need real-time progress updates from background operations
3. You're running tasks that potentially continue forever (like monitoring)

## Interactive AI Patterns

MCPC enables powerful interactive patterns that weren't possible before in MCP:

- **Modifying running tasks**: You can adjust parameters or change the behavior of a task while it's running (e.g., "focus on this subset of data instead" or "parse the PDF first")
- **Tool-initiated prompts**: A tool can ask for clarification when it encounters ambiguity or needs additional input
- **Conversation branching**: Start multiple background tasks and selectively respond to their updates while maintaining conversational context
- **Proactive AI Actions**: Your MCP server can notify the LLM of events, allowing it to take action (e.g., "Database migration completed" → LLM runs verification query)

## Ideal Use Cases

MCPC is ideal for:

- **Interactive AI Agents**: Chat with LLMs while tasks run in the background
- **Data Processing**: Stream progress updates during large file processing
- **Content Generation**: Receive partial results as they're generated
- **Long-Running Operations**: Support for tasks that run indefinitely
- **Distributed Systems**: Coordinate asynchronous operations across services
- **Proactive AI**: Let LLMs respond to events and take action automatically
- **Automated Workflows**: Create self-managing systems that adapt to events
- **Intelligent Monitoring**: AI agents that actively respond to system changes

## Example Scenarios

### Data Processing with Progress Updates

An LLM agent initiates a large file processing task, and the user can continue to chat with it while receiving real-time updates on the processing progress. When processing completes, the LLM can automatically analyze the results.

### Continuous Monitoring with AI Intervention

An LLM agent monitors a system in real-time through MCPC server events. When it detects anomalies, it can proactively respond and take remedial actions without human intervention.

### Multi-Service Orchestration

An LLM agent coordinates multiple background tasks across different services, managing their dependencies and responding to completion events to orchestrate a complex workflow.

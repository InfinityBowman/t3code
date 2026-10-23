// Tests run inside a T3 session inherit its keeper socket: they would then launch and kill Claude processes on
// the production keeper, and recovery would take its live sessions for the fixtures'.
delete process.env.T3CODE_CLAUDE_KEEPER_SOCKET;

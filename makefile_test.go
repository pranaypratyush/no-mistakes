package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestMakeBuildPrioritizesDotEnvUmamiWebsiteID(t *testing.T) {
	skipMakeBuildTestsOnWindows(t)

	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Skip("make not available")
	}

	workDir := writeTestMakeWorkspace(t)
	if err := os.WriteFile(filepath.Join(workDir, ".env"), []byte("NO_MISTAKES_UMAMI_WEBSITE_ID=website-from-dotenv\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	output := runMakeDryBuild(t, makePath, workDir, map[string]string{
		"UMAMI_WEBSITE_ID": "website-from-env",
	})

	if !strings.Contains(output, "TelemetryWebsiteID=website-from-dotenv") {
		t.Fatalf("make build output should embed .env website id, got:\n%s", output)
	}
	if strings.Contains(output, "TelemetryWebsiteID=website-from-env") {
		t.Fatalf("make build output should not prefer env website id when .env exists, got:\n%s", output)
	}
}

func TestMakeBuildUsesEnvUmamiWebsiteIDWhenDotEnvMissing(t *testing.T) {
	skipMakeBuildTestsOnWindows(t)

	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Skip("make not available")
	}

	workDir := writeTestMakeWorkspace(t)
	output := runMakeDryBuild(t, makePath, workDir, map[string]string{
		"UMAMI_WEBSITE_ID": "website-from-env",
	})

	if !strings.Contains(output, "TelemetryWebsiteID=website-from-env") {
		t.Fatalf("make build output should embed env website id when .env is absent, got:\n%s", output)
	}
}

func TestMakeBuildEmbedsDefaultSelfHostedTelemetryConfig(t *testing.T) {
	skipMakeBuildTestsOnWindows(t)

	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Skip("make not available")
	}

	workDir := writeTestMakeWorkspace(t)
	output := runMakeDryBuild(t, makePath, workDir, nil)

	if !strings.Contains(output, "TelemetryHost=https://a.kunchenguid.com") {
		t.Fatalf("make build output should embed default telemetry host, got:\n%s", output)
	}
	if !strings.Contains(output, "TelemetryWebsiteID=f959e889-92f5-4121-8a1f-571b10861198") {
		t.Fatalf("make build output should embed default telemetry website id, got:\n%s", output)
	}
}

func TestMakeBuildPrioritizesDotEnvUmamiHost(t *testing.T) {
	skipMakeBuildTestsOnWindows(t)

	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Skip("make not available")
	}

	workDir := writeTestMakeWorkspace(t)
	if err := os.WriteFile(filepath.Join(workDir, ".env"), []byte("NO_MISTAKES_UMAMI_HOST=https://dotenv.example\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	output := runMakeDryBuild(t, makePath, workDir, map[string]string{
		"UMAMI_HOST": "https://env.example",
	})

	if !strings.Contains(output, "TelemetryHost=https://dotenv.example") {
		t.Fatalf("make build output should embed .env telemetry host, got:\n%s", output)
	}
	if strings.Contains(output, "TelemetryHost=https://env.example") {
		t.Fatalf("make build output should not prefer env telemetry host when .env exists, got:\n%s", output)
	}
}

func TestMakeBuildIgnoresUnrelatedDotEnvEntries(t *testing.T) {
	skipMakeBuildTestsOnWindows(t)

	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Skip("make not available")
	}

	workDir := writeTestMakeWorkspace(t)
	if err := os.WriteFile(filepath.Join(workDir, ".env"), []byte("VERSION=from-dotenv\nNO_MISTAKES_UMAMI_WEBSITE_ID=website-from-dotenv\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	output := runMakeDryBuild(t, makePath, workDir, nil)

	if !strings.Contains(output, "TelemetryWebsiteID=website-from-dotenv") {
		t.Fatalf("make build output should still embed dotenv website id, got:\n%s", output)
	}
	if strings.Contains(output, "/internal/buildinfo.Version=from-dotenv") {
		t.Fatalf("make build should ignore unrelated dotenv entries, got:\n%s", output)
	}
}

func TestMakeBuildStripsInlineCommentsFromDotEnvUmamiWebsiteID(t *testing.T) {
	skipMakeBuildTestsOnWindows(t)

	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Skip("make not available")
	}

	workDir := writeTestMakeWorkspace(t)
	if err := os.WriteFile(filepath.Join(workDir, ".env"), []byte("NO_MISTAKES_UMAMI_WEBSITE_ID=website-from-dotenv # dev\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	output := runMakeDryBuild(t, makePath, workDir, nil)

	if !strings.Contains(output, "TelemetryWebsiteID=website-from-dotenv") {
		t.Fatalf("make build output should strip inline comments from dotenv website id, got:\n%s", output)
	}
	if strings.Contains(output, "TelemetryWebsiteID=website-from-dotenv # dev") {
		t.Fatalf("make build output should not embed inline comments in website id, got:\n%s", output)
	}
}

func TestMakeBuildPreservesQuotedHashInDotEnvUmamiWebsiteID(t *testing.T) {
	skipMakeBuildTestsOnWindows(t)

	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Skip("make not available")
	}

	workDir := writeTestMakeWorkspace(t)
	if err := os.WriteFile(filepath.Join(workDir, ".env"), []byte("NO_MISTAKES_UMAMI_WEBSITE_ID=\"website # dev\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	output := runMakeDryBuild(t, makePath, workDir, nil)

	if !strings.Contains(output, "TelemetryWebsiteID=website # dev") {
		t.Fatalf("make build output should preserve quoted hashes in dotenv website id, got:\n%s", output)
	}
	if strings.Contains(output, "TelemetryWebsiteID=\"website") {
		t.Fatalf("make build output should not truncate quoted dotenv website id, got:\n%s", output)
	}
}

func TestE2ETargetIncludesNativeAgentIntegrations(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("scripts", "e2e.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "./internal/agent/...") {
		t.Fatal("scripts/e2e.sh default package sweep must include native agent integrations")
	}
	agentGuidance := readRepoFile(t, "AGENTS.md")
	if !strings.Contains(agentGuidance, "`scripts/e2e.sh` owns the default package sweep") {
		t.Fatal("AGENTS.md must point to scripts/e2e.sh instead of caching its package list")
	}
	if strings.Contains(agentGuidance, "which sweeps `./internal/e2e/...`") {
		t.Fatal("AGENTS.md contains the stale partial e2e package list")
	}
}

func TestCodexAppServerAttachDocsHaveSingleOwner(t *testing.T) {
	globalConfig := readRepoFile(t, "docs", "src", "content", "docs", "reference", "global-config.md")
	cliReference := readRepoFile(t, "docs", "src", "content", "docs", "reference", "cli.md")
	agentGuide := readRepoFile(t, "docs", "src", "content", "docs", "guides", "agents.md")
	pipelineSteps := readRepoFile(t, "docs", "src", "content", "docs", "reference", "pipeline-steps.md")

	attachCommand := "codex --remote <codex.app_server_endpoint> resume <session_id>"
	for name, document := range map[string]string{"global config": globalConfig, "agent guide": agentGuide, "pipeline steps": pipelineSteps} {
		if strings.Contains(document, "codex --remote") || strings.Contains(document, "active_steps[].session_id") {
			t.Fatalf("%s duplicates live-attach semantics owned by the CLI reference", name)
		}
	}
	_, stepStatusReference, found := strings.Cut(pipelineSteps, "## Step statuses")
	if !found {
		t.Fatal("pipeline steps must retain its step-status reference section")
	}
	for _, identityTerm := range []string{"session", "thread id"} {
		if strings.Contains(strings.ToLower(stepStatusReference), identityTerm) {
			t.Fatalf("pipeline step-status reference duplicates %q identity semantics owned by the CLI reference", identityTerm)
		}
	}
	if strings.Count(cliReference, attachCommand) != 1 {
		t.Fatalf("CLI reference must contain the endpoint-aware attach command exactly once")
	}
	if !strings.Contains(cliReference, "`active_steps`") || !strings.Contains(cliReference, "`session_id`") {
		t.Fatal("CLI reference must own the active session field")
	}
	if !strings.Contains(globalConfig, "/no-mistakes/reference/cli/#no-mistakes-axi-status") {
		t.Fatal("global config reference must link to the CLI owner of live session semantics")
	}
	if !strings.Contains(agentGuide, "/no-mistakes/reference/global-config/#codex") || !strings.Contains(agentGuide, "/no-mistakes/reference/cli/#no-mistakes-axi-status") {
		t.Fatal("agent guide must link to both Codex configuration and CLI session owners")
	}
	if !strings.Contains(pipelineSteps, "/no-mistakes/reference/cli/#no-mistakes-axi-status") {
		t.Fatal("pipeline steps must link to the CLI owner of active_steps output semantics")
	}
	var codexProtocolRow string
	for _, line := range strings.Split(agentGuide, "\n") {
		if strings.HasPrefix(line, "| Codex |") {
			codexProtocolRow = line
			break
		}
	}
	for _, term := range []string{"Subprocess", "App Server", "/no-mistakes/reference/global-config/#codex"} {
		if !strings.Contains(codexProtocolRow, term) {
			t.Fatalf("Codex protocol row must acknowledge both transports and link the config owner; missing %q in %q", term, codexProtocolRow)
		}
	}
	if strings.Contains(agentGuide, "One-shot subprocess agents (Claude, Codex,") {
		t.Fatal("agent guide classifies Codex as subprocess-only despite its optional App Server transport")
	}
}

func readRepoFile(t *testing.T, elements ...string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(elements...))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func skipMakeBuildTestsOnWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("make build tests are POSIX-oriented")
	}
}

func writeTestMakeWorkspace(t *testing.T) string {
	t.Helper()

	data, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatal(err)
	}
	workDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workDir, "Makefile"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	return workDir
}

func runMakeDryBuild(t *testing.T, makePath, workDir string, extraEnv map[string]string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, makePath, "-n", "build")
	cmd.Dir = workDir
	cmd.Env = filteredEnv(os.Environ(), "UMAMI_HOST", "UMAMI_WEBSITE_ID", "NO_MISTAKES_UMAMI_HOST", "NO_MISTAKES_UMAMI_WEBSITE_ID")
	for key, value := range extraEnv {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make -n build failed: %v\n%s", err, out)
	}
	return string(out)
}

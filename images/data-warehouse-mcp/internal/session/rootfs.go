package session

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

func extractRootfs(ctx context.Context, archivePath, destination string) error {
	// --no-same-owner prevents tar from calling chown to restore original ownership,
	// which would fail because the container drops CAP_CHOWN (only SYS_CHROOT is added back).
	cmd := exec.CommandContext(ctx, "tar", "--no-same-owner", "-xf", archivePath, "-C", destination)
	output, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message != "" {
			return fmt.Errorf("extract rootfs: %w: %s", err, message)
		}
		return fmt.Errorf("extract rootfs: %w", err)
	}
	return nil
}

import configparser
import subprocess
from pathlib import Path
from typing import Optional


class Emailer:
    """Handle email configuration, template rendering and delivery."""

    def __init__(self, config_file: Path):
        self.config_file = Path(config_file)

        if not self.config_file.exists():
            raise FileNotFoundError(
                f"Email configuration file not found: "
                f"{self.config_file}"
            )

        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)

        if "email" not in self.config:
            raise ValueError(
                "Missing [email] section in "
                f"{self.config_file}"
            )

        email_config = self.config["email"]

        self.sender = email_config.get(
            "sender",
            fallback="",
        ).strip()

        self.cc = self._split_addresses(
            email_config.get(
                "cc",
                fallback="",
            )
        )

        self.bcc = self._split_addresses(
            email_config.get(
                "bcc",
                fallback="",
            )
        )

        self.subject = email_config.get(
            "subject",
            fallback="Slurm Job Performance Report",
        ).strip()

        if not self.sender:
            raise ValueError(
                "Email sender is not configured."
            )

        if not self.subject:
            raise ValueError(
                "Email subject is not configured."
            )

    @staticmethod
    def _split_addresses(value: str):
        """
        Convert comma/semicolon-separated addresses into a list.
        Empty values result in an empty list.
        """

        if not value:
            return []

        value = value.replace(";", ",")

        return [
            address.strip()
            for address in value.split(",")
            if address.strip()
        ]

    @staticmethod
    def render_template(
        template_file: Path,
        **variables,
    ) -> str:
        """
        Load an HTML template and substitute Python variables.

        The template uses normal Python str.format() syntax:

            {user}
            {num_report}
            {total_jobs}
            {job_info}
        """

        template_file = Path(template_file)

        if not template_file.exists():
            raise FileNotFoundError(
                f"Email template not found: {template_file}"
            )

        template = template_file.read_text(
            encoding="utf-8",
        )

        try:
            return template.format(**variables)
        except KeyError as exc:
            raise ValueError(
                f"Unknown template variable: {exc}"
            ) from exc

    def build_mail_command(
        self,
        recipient: str,
    ):
        """Build the mail command."""

        command = [
            "mail",
            "-s",
            self.subject,
            "-r",
            self.sender,
            "-S",
            "Content-Type: text/html; charset=UTF-8",
            "-S",
            "Content-Transfer-Encoding: quoted-printable",
        ]

        if self.cc:
            command.extend(
                [
                    "-c",
                    ",".join(self.cc),
                ]
            )

        if self.bcc:
            command.extend(
                [
                    "-b",
                    ",".join(self.bcc),
                ]
            )

        command.append(recipient)

        return command

    def send(
        self,
        recipient: str,
        body: str,
    ):
        """Send an HTML email."""

        if not recipient:
            raise ValueError(
                "Cannot send email without a recipient."
            )

        command = self.build_mail_command(recipient)

        result = subprocess.run(
            command,
            input=body,
            text=True,
            capture_output=True,
            check=False,
        )

        if result.returncode != 0:
            error = result.stderr.strip()

            raise RuntimeError(
                f"mail command failed "
                f"(exit code {result.returncode}): "
                f"{error}"
            )


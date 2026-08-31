import os
import subprocess

from .utils import project_root


class EmailError(RuntimeError):
    """Raised when email delivery fails."""


class Emailer(object):
    """
    Email delivery and template handling.

    Email configuration is kept in slurm_report.conf and the actual
    message body is kept in an external HTML template.
    """

    def __init__(self, config):
        self.config = config

        self.sender = config.get(
            "email",
            "sender",
            fallback="",
        ).strip()

        self.cc = self._address_list(
            config.get(
                "email",
                "cc",
                fallback="",
            )
        )

        self.bcc = self._address_list(
            config.get(
                "email",
                "bcc",
                fallback="",
            )
        )

        self.fallback_recipient = config.get(
            "email",
            "fallback_recipient",
            fallback="",
        ).strip()

        self.mail_command = config.get(
            "email",
            "mail_command",
            fallback="mail",
        ).strip()

        self.job_subject = config.get(
            "email",
            "job_subject",
            fallback="Slurm Job Performance Report",
        ).strip()

        self.node_subject = config.get(
            "email",
            "node_subject",
            fallback="Slurm Node Performance Alert",
        ).strip()

        self.job_template = self._template_path(
            config.get(
                "templates",
                "job_report",
                fallback="job_report.html",
            )
        )

        self.node_template = self._template_path(
            config.get(
                "templates",
                "node_alert",
                fallback="node_alert.html",
            )
        )

    @staticmethod
    def _address_list(value):
        """
        Convert a comma-separated address string into a list.
        """
        if not value:
            return []

        return [
            item.strip()
            for item in value.split(",")
            if item.strip()
        ]

    @staticmethod
    def _template_path(filename):
        """
        Resolve a template relative to the project directory.
        """
        filename = os.path.expandvars(
            os.path.expanduser(filename.strip())
        )

        if os.path.isabs(filename):
            return filename

        return os.path.join(
            project_root(),
            filename,
        )

    @staticmethod
    def render_template(path, variables):
        """
        Render a template using Python str.format().

        This intentionally keeps the templating mechanism simple:
        users can edit the HTML directly and use {variable} placeholders.
        """
        if not os.path.isfile(path):
            raise EmailError(
                "Email template not found: {}".format(path)
            )

        with open(path, "r") as handle:
            template = handle.read()

        try:
            return template.format(**variables)
        except KeyError as exc:
            raise EmailError(
                "Missing template variable: {}".format(exc)
            )

    def _build_mail_command(
        self,
        recipient,
        subject,
    ):
        """
        Build the mail command.

        No shell is used, so addresses and subjects are not interpreted
        as shell commands.
        """
        if not recipient:
            raise EmailError("No email recipient specified.")

        command = [
            self.mail_command,
            "-s",
            subject,
        ]

        if self.sender:
            command.extend([
                "-r",
                self.sender,
            ])

        if self.cc:
            command.extend([
                "-c",
                ",".join(self.cc),
            ])

        if self.bcc:
            command.extend([
                "-b",
                ",".join(self.bcc),
            ])

        command.append(recipient)

        return command

    def send(
        self,
        recipient,
        subject,
        html_body,
    ):
        """
        Send an HTML email through the configured mail command.
        """
        command = self._build_mail_command(
            recipient,
            subject,
        )

        try:
            result = subprocess.run(
                command,
                input=html_body,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=False,
            )
        except OSError as exc:
            raise EmailError(
                "Unable to execute mail command '{}': {}".format(
                    self.mail_command,
                    exc,
                )
            )

        if result.returncode != 0:
            raise EmailError(
                "Email delivery failed for {}: {}".format(
                    recipient,
                    result.stderr.strip(),
                )
            )

    def send_job_report(
        self,
        recipient,
        variables,
    ):
        """
        Render and send a job-performance report.
        """
        subject = self.job_subject.format(**variables)

        body = self.render_template(
            self.job_template,
            variables,
        )

        self.send(
            recipient,
            subject,
            body,
        )

    def send_node_alert(
        self,
        recipient,
        variables,
    ):
        """
        Render and send a node-performance alert.
        """
        subject = self.node_subject.format(**variables)

        body = self.render_template(
            self.node_template,
            variables,
        )

        self.send(
            recipient,
            subject,
            body,
        )


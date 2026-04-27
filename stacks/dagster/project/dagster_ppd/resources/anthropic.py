import anthropic
from dagster import ConfigurableResource, EnvVar


class AnthropicResource(ConfigurableResource):
    api_key: str = EnvVar("ANTHROPIC_API_KEY")

    def get_client(self) -> anthropic.Anthropic:
        return anthropic.Anthropic(api_key=self.api_key)

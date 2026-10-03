from pydantic import model_validator
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    db_name: str
    db_host: str
    db_password: str
    db_user: str
    base_log_dir: str
    pool_instance_name: str

    # Fenetre de calcul de la repartition, alignee sur l'ancien script.
    distribution_window_days: int = 14

    # Fenetre d'ingestion: au-dela, un sharelog n'est plus surveille. Doit
    # rester >= distribution_window_days, la marge absorbant un retard
    # d'ingestion (exporteur arrete, reprise de fichiers en attente) sans que
    # des shares encore dans la fenetre de calcul cessent d'etre suivies.
    #
    # Le validateur ci-dessous ne controle que la borne basse. La borne haute est
    # le start_offset du CAgg horaire (17 jours, code en dur dans
    # 002_hypertables.sql) et n'est verifiee par rien : monter cette valeur
    # au-dela de 16 sans toucher a 002 fait disparaitre des donnees de l'agregat,
    # sans erreur.
    monitor_window_days: int = 16

    # Au-dela, worker_C remet en PENDING un fichier reste en PROCESSING.
    processing_timeout_seconds: int = 300

    model_config = {"env_file": ".env", "extra": "allow"}

    @model_validator(mode = "after")
    def _windows_are_consistent(self):
        if self.monitor_window_days < self.distribution_window_days:
            raise ValueError(
                "monitor_window_days doit etre >= distribution_window_days, sinon des "
                "sharelogs cessent d'etre suivis alors que leurs shares comptent encore"
            )
        return self


settings = Settings()  # type: ignore[call-arg]

package run.trama.saga

object SagaDefinitionValidator {
    fun validate(definition: SagaDefinition): List<String> {
        val errors = mutableListOf<String>()

        if (definition.name.isBlank()) {
            errors.add("name must not be blank")
        }
        if (definition.version.isBlank()) {
            errors.add("version must not be blank")
        }
        if (definition.steps.isEmpty()) {
            errors.add("steps must not be empty")
        }

        definition.onSuccessCallback?.let {
            errors.addAll(validateHttpCall("onSuccessCallback", it))
        }
        definition.onFailureCallback?.let {
            errors.addAll(validateHttpCall("onFailureCallback", it))
        }

        val stepNames = mutableSetOf<String>()
        definition.steps.forEachIndexed { index, step ->
            if (step.name.isBlank()) {
                errors.add("steps[$index].name must not be blank")
            } else if (!stepNames.add(step.name)) {
                errors.add("steps[$index].name must be unique")
            }

            errors.addAll(validateHttpCall("steps[$index].up", step.up))
            errors.addAll(validateHttpCall("steps[$index].down", step.down))
        }

        return errors
    }

    private fun validateHttpCall(path: String, call: HttpCall): List<String> {
        val errors = mutableListOf<String>()

        if (call.url.value.isBlank()) {
            errors.add("$path.url must not be blank")
        }

        call.headers.forEach { (key, value) ->
            if (key.isBlank()) {
                errors.add("$path.headers contains a blank header name")
            }
            if (value.value.isBlank()) {
                errors.add("$path.headers[$key] must not be blank")
            }
        }

        call.body?.let {
            if (it.value.isBlank()) {
                errors.add("$path.body must not be blank when provided")
            }
        }

        if (call.successStatusCodes.isEmpty()) {
            errors.add("$path.successStatusCodes must not be empty")
        } else {
            call.successStatusCodes.forEach { code ->
                if (code !in 100..599) {
                    errors.add("$path.successStatusCodes contains invalid code: $code")
                }
            }
        }

        return errors
    }
}
